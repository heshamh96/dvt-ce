"""
Install dbt adapter packages based on adapter types found in profiles.yml.

IMPORTANT: dbt adapter packages (dbt-postgres, dbt-snowflake, etc.) declare
dbt-core as a dependency. When pip installs them, it pulls dbt-core from PyPI.
This conflicts with dvt-ce, which provides the same dbt.* namespace.
dvt sync ALWAYS checks for and removes dbt-core to resolve the conflict.
"""

import shutil
import subprocess
import sys
from typing import Dict, List, Optional, Set, Tuple

# Maps adapter type → pip package name
ADAPTER_TO_PACKAGE: Dict[str, str] = {
    "postgres": "dbt-postgres",
    "snowflake": "dbt-snowflake",
    "bigquery": "dbt-bigquery",
    "redshift": "dbt-redshift",
    "mysql": "dbt-mysql",
    "sqlserver": "dbt-sqlserver",
    "databricks": "dbt-databricks",
    "oracle": "dbt-oracle",
    "trino": "dbt-trino",
    "clickhouse": "dbt-clickhouse",
    "duckdb": "dbt-duckdb",
    "sqlite": "dbt-sqlite",
    "spark": "dbt-spark",
    "fabric": "dbt-fabric",
    "exasol": "dbt-exasol",
    "teradata": "dbt-teradata",
    "vertica": "dbt-vertica",
}

# Adapter connector dependencies that need separate installation
# (because we install adapter packages with --no-deps to avoid dbt-core)
# These include both the database connector AND any other deps the adapter needs
ADAPTER_CONNECTOR_DEPS: Dict[str, List[str]] = {
    "postgres": ["psycopg2-binary"],
    "snowflake": ["snowflake-connector-python"],
    "bigquery": ["google-cloud-bigquery"],
    "redshift": ["redshift-connector"],
    "mysql": ["mysql-connector-python"],
    "sqlserver": ["pyodbc", "azure-identity"],
    "databricks": ["databricks-sql-connector", "databricks-sdk", "sqlparams"],
    "oracle": ["oracledb"],
    "trino": ["trino"],
    "clickhouse": ["clickhouse-driver"],
    "duckdb": ["duckdb"],
    "spark": ["pyhive", "thrift", "sqlparams"],
    "fabric": ["pyodbc"],
}


def _pip_cmd() -> List[str]:
    """Return the pip command to use: uv pip or python -m pip."""
    uv = shutil.which("uv")
    if uv:
        return [uv, "pip"]
    return [sys.executable, "-m", "pip"]


def _pip_install(package: str, extra_args: Optional[List[str]] = None) -> bool:
    """Install a package via pip/uv. Returns True on success."""
    cmd = _pip_cmd() + ["install", package, "--quiet"]
    if extra_args:
        cmd.extend(extra_args)
    # For uv, specify the current python
    if "uv" in cmd[0]:
        cmd.extend(["--python", sys.executable])
    try:
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False


def _pip_uninstall(package: str) -> bool:
    """Uninstall a package via pip/uv. Returns True on success."""
    base = _pip_cmd()
    cmd = base + ["uninstall", package]
    if "uv" in base[0]:
        # uv doesn't use -y, it just uninstalls
        cmd.extend(["--python", sys.executable])
    else:
        # pip needs -y to skip confirmation
        cmd.append("-y")
        cmd.append("--quiet")
    try:
        subprocess.check_call(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False


def get_required_packages(adapter_types: Set[str]) -> List[str]:
    """Map adapter types to pip package names."""
    packages = []
    for adapter_type in sorted(adapter_types):
        pkg = ADAPTER_TO_PACKAGE.get(adapter_type)
        if pkg:
            packages.append(pkg)
    return packages


def check_installed(package: str) -> bool:
    """Check if a pip package is already installed (via metadata, not import)."""
    try:
        import importlib.metadata as meta

        meta.distribution(package)
        return True
    except (meta.PackageNotFoundError, Exception):
        return False


def install_packages(
    packages: List[str], dry_run: bool = False
) -> List[Tuple[str, str]]:
    """Install dbt adapter packages safely without pulling dbt-core.

    Strategy:
    1. Install the adapter's connector dependency normally (e.g., psycopg2-binary)
    2. Install the adapter package itself with --no-deps (to avoid pulling dbt-core)
    3. Purge dbt-core if it sneaked in anyway

    Status is one of: 'installed', 'already_installed', 'failed', 'dry_run'
    """
    results = []

    for package in packages:
        if check_installed(package):
            results.append((package, "already_installed"))
            continue

        if dry_run:
            results.append((package, "dry_run"))
            continue

        # Find the adapter type for this package
        adapter_type = next(
            (k for k, v in ADAPTER_TO_PACKAGE.items() if v == package), None
        )

        # Step 1: Install connector dependencies (normal install, these are safe)
        if adapter_type and adapter_type in ADAPTER_CONNECTOR_DEPS:
            for dep in ADAPTER_CONNECTOR_DEPS[adapter_type]:
                _pip_install(dep)  # best-effort, don't fail on connector deps

        # Step 2: Install adapter with --no-deps (avoids pulling dbt-core)
        if _pip_install(package, ["--no-deps"]):
            results.append((package, "installed"))
        else:
            results.append((package, "failed"))

    # Step 3: Safety net — purge dbt-core if it sneaked in
    if not dry_run and is_dbt_core_installed():
        purge_dbt_core()

    return results


# ---------------------------------------------------------------------------
# dbt-core conflict detection and removal
# ---------------------------------------------------------------------------


def is_dbt_core_installed() -> bool:
    """Check if the PyPI dbt-core package is installed.

    Returns True if dbt-core (the PyPI package) is present. This is always
    a conflict when dvt-ce is installed, since both provide dbt.*.
    """
    try:
        import importlib.metadata as meta

        installed = {d.metadata["Name"].lower() for d in meta.distributions()}
        return "dbt-core" in installed
    except Exception:
        return False


def check_dbt_core_conflict() -> bool:
    """Check if dbt-core is installed alongside dvt-ce (conflict)."""
    try:
        import importlib.metadata as meta

        installed = {d.metadata["Name"].lower() for d in meta.distributions()}
        return "dbt-core" in installed and "dvt-ce" in installed
    except Exception:
        return False


def purge_dbt_core(dry_run: bool = False) -> bool:
    """Uninstall dbt-core if it's present.

    dbt adapter packages (dbt-postgres, etc.) pull dbt-core from PyPI as a
    dependency. Since dvt-ce provides the same dbt.* namespace, having both
    installed causes import conflicts. This function removes dbt-core,
    leaving dvt-ce as the sole provider of dbt.*.

    This is safe to call at any time — if dbt-core isn't installed, it's a no-op.

    Returns True if dbt-core was removed (or would be in dry_run).
    """
    if not is_dbt_core_installed():
        return False

    if dry_run:
        return True

    return _pip_uninstall("dbt-core")
