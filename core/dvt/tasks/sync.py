"""
DVT Sync Task — environment bootstrap and self-healing.

Reads profiles.yml and ensures the environment is correct:
1. Purge dbt-core (conflicts with dvt-ce, adapters pull it from PyPI)
2. Install required dbt adapter packages
3. Purge dbt-core AGAIN (adapters may have re-pulled it)
4. Verify adapters still work after purge
5. Install cloud SDKs for bucket connections
6. Install DuckDB extensions
7. Verify Sling binary

dvt sync is designed to be run repeatedly and to self-heal:
- If user accidentally installs dbt-core → sync removes it
- If adapter install breaks after purge → sync reinstalls it
- If new adapters are added to profiles.yml → sync installs them
"""

from typing import Any, Dict, List, Tuple

from dvt.sync.profiles_reader import (
    default_profiles_dir,
    extract_outputs,
    get_adapter_types,
    read_profiles_yml,
)
from dvt.sync.adapter_installer import (
    ADAPTER_TO_PACKAGE,
    check_installed,
    get_required_packages,
    install_packages,
    is_dbt_core_installed,
    purge_dbt_core,
    _pip_install,
)
from dvt.sync.duckdb_extensions import get_required_extensions, install_extensions
from dvt.sync.cloud_deps import get_required_cloud_packages, install_cloud_packages
from dvt.sync.sling_checker import check_sling


def _status_icon(status: str) -> str:
    if status in ("already_installed", "installed", "verified"):
        return "[installed]"
    elif status == "repaired":
        return "[repaired]"
    elif status == "failed":
        return "[FAILED]"
    elif status == "dry_run":
        return "[dry-run]"
    elif status == "removed":
        return "[removed]"
    elif status == "not_found":
        return "[NOT FOUND]"
    return f"[{status}]"


def _check_adapter_importable(adapter_type: str) -> bool:
    """Check if a dbt adapter can be imported."""
    try:
        # dbt adapters register under dbt.adapters.<type>
        __import__(f"dbt.adapters.{adapter_type}")
        return True
    except (ImportError, Exception):
        return False


class DvtSyncTask:
    """Bootstrap and self-heal the DVT environment from profiles.yml."""

    def __init__(self, flags: Any, cli_kwargs: Dict[str, Any]) -> None:
        self.flags = flags
        self.skip_test = cli_kwargs.get("skip_test", False)
        self.dry_run = cli_kwargs.get("dry_run", False)
        self.profiles_dir = (
            getattr(flags, "PROFILES_DIR", None) or default_profiles_dir()
        )

    def run(self) -> Any:
        print(f"\ndvt sync — reading profiles from: {self.profiles_dir}\n")
        all_ok = True

        # ---------------------------------------------------------------
        # Step 0: Purge dbt-core if present (always, before anything)
        # ---------------------------------------------------------------
        if is_dbt_core_installed():
            if self.dry_run:
                print("  dbt-core conflict:")
                print("    dbt-core ...................... [would remove]")
            else:
                print("  dbt-core conflict: removing (dvt-ce replaces it)")
                if purge_dbt_core():
                    print("    dbt-core ...................... [removed]")
                else:
                    print("    dbt-core ...................... [FAILED to remove]")
                    all_ok = False
            print()

        # ---------------------------------------------------------------
        # Step 1: Read profiles.yml
        # ---------------------------------------------------------------
        try:
            raw_profiles = read_profiles_yml(self.profiles_dir)
        except FileNotFoundError as e:
            print(f"  ERROR: {e}")
            print("  Run 'dvt init' to create a project, or use --profiles-dir.")
            return {"success": False}
        except Exception as e:
            print(f"  ERROR reading profiles.yml: {e}")
            return {"success": False}

        outputs = extract_outputs(raw_profiles)
        adapter_types = get_adapter_types(outputs)

        if not adapter_types:
            print("  No outputs found in profiles.yml. Nothing to sync.")
            return {"success": True}

        print(f"  Found {len(outputs)} outputs across {len(raw_profiles)} profile(s)")
        print(f"  Adapter types: {', '.join(sorted(adapter_types))}")
        print()

        # ---------------------------------------------------------------
        # Step 2: Install adapters + purge dbt-core + verify
        # ---------------------------------------------------------------
        db_types = {
            t for t in adapter_types if t not in {"s3", "gcs", "azure", "local"}
        }
        adapter_packages = get_required_packages(db_types)

        print("  Adapters:")
        if adapter_packages:
            results = self._install_and_verify_adapters(db_types, adapter_packages)
            for name, status in results:
                pad = "." * max(1, 30 - len(name))
                print(f"    {name} {pad} {_status_icon(status)}")
                if status == "failed":
                    all_ok = False
        else:
            print("    (no database adapters needed)")
        print()

        # ---------------------------------------------------------------
        # Step 3: Install cloud SDKs
        # ---------------------------------------------------------------
        bucket_types = {t for t in adapter_types if t in {"s3", "gcs", "azure"}}
        if bucket_types:
            print("  Buckets:")
            cloud_packages = get_required_cloud_packages(bucket_types)
            results = install_cloud_packages(cloud_packages, dry_run=self.dry_run)
            for pkg, status in results:
                pad = "." * max(1, 30 - len(pkg))
                print(f"    {pkg} {pad} {_status_icon(status)}")
                if status == "failed":
                    all_ok = False
            print()

        # ---------------------------------------------------------------
        # Step 4: Install DuckDB extensions
        # ---------------------------------------------------------------
        print("  DuckDB:")
        try:
            import duckdb

            pad = "." * 24
            print(f"    core {pad} duckdb {duckdb.__version__} [installed]")
        except ImportError:
            pad = "." * 24
            print(f"    core {pad} [MISSING — install duckdb]")
            all_ok = False

        extensions = get_required_extensions(adapter_types)
        results = install_extensions(extensions, dry_run=self.dry_run)
        for ext, status in results:
            pad = "." * max(1, 30 - len(ext))
            print(f"    {ext} {pad} {_status_icon(status)}")
            if status == "failed":
                all_ok = False
        print()

        # ---------------------------------------------------------------
        # Step 5: Check Sling
        # ---------------------------------------------------------------
        print("  Sling:")
        sling_available, sling_version = check_sling()
        pad = "." * 24
        if sling_available:
            print(f"    binary {pad} sling {sling_version} [installed]")
        else:
            print(f"    binary {pad} [NOT FOUND]")
            print("    Install: https://docs.slingdata.io/sling-cli/getting-started")
            print("    Sling is required for cross-engine extraction.")
        print()

        # ---------------------------------------------------------------
        # Summary
        # ---------------------------------------------------------------
        if all_ok:
            print("  Sync complete. Environment ready.")
        else:
            print("  Sync completed with errors. Check output above.")

        return {"success": all_ok}

    def _install_and_verify_adapters(
        self,
        db_types: set,
        adapter_packages: List[str],
    ) -> List[Tuple[str, str]]:
        """Install adapters, purge dbt-core, verify imports, repair if broken.

        Uses install_packages() from adapter_installer which:
        1. Installs connector deps normally (psycopg2, snowflake-connector, etc.)
        2. Installs adapter packages with --no-deps (avoids pulling dbt-core)
        3. Purges dbt-core if it sneaked in

        After that, we verify each adapter imports and repair if needed.
        """
        # Phase 1: Install via adapter_installer (handles --no-deps + connector deps)
        results = install_packages(adapter_packages, dry_run=self.dry_run)

        if self.dry_run:
            return results

        # Phase 2: Verify each adapter imports, repair if broken
        for i, (pkg, status) in enumerate(results):
            if status == "failed":
                continue

            adapter_type = next(
                (k for k, v in ADAPTER_TO_PACKAGE.items() if v == pkg), None
            )
            if not adapter_type:
                continue

            if not _check_adapter_importable(adapter_type):
                # Adapter broke — try reinstall with --no-deps --force-reinstall
                if _pip_install(pkg, ["--no-deps", "--force-reinstall"]):
                    if is_dbt_core_installed():
                        purge_dbt_core()
                    if _check_adapter_importable(adapter_type):
                        results[i] = (pkg, "repaired")
                    else:
                        results[i] = (pkg, "failed")
                else:
                    results[i] = (pkg, "failed")

        return results

    @staticmethod
    def interpret_results(results: Any) -> bool:
        if results is None:
            return False
        if isinstance(results, dict):
            return results.get("success", False)
        return True
