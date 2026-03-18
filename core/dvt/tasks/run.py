"""
DVT Run Task — extends dbt's RunTask with cross-engine extraction.

Manages the persistent DuckDB cache (.dvt/cache.duckdb) and dispatches
DvtModelRunner for extraction models.

--full-refresh destroys the cache and re-extracts everything.
"""

import logging
from typing import Dict, Optional, Type

from dbt.task.run import ModelRunner, RunTask

from dvt.config.source_connections import (
    load_source_connections,
    validate_source_connections,
)
from dvt.config.target_resolver import (
    ExecutionPath,
    ResolvedModel,
    resolve_all_models,
)
from dvt.federation.dvt_cache import DvtCache
from dvt.runners.model_runner import DvtModelRunner
from dvt.sync.profiles_reader import default_profiles_dir, read_profiles_yml

logger = logging.getLogger(__name__)


class DvtRunTask(RunTask):
    """Extends RunTask with DVT extraction via persistent DuckDB cache.

    Lifecycle:
    1. Reads profiles.yml + sources.yml
    2. Resolves execution paths for all models
    3. If --full-refresh: destroys the DuckDB cache
    4. Opens/creates the DuckDB cache at .dvt/cache.duckdb
    5. For each extraction model: DvtModelRunner uses the shared cache
    6. Cache persists for incremental support on next run
    """

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self._resolutions: Dict[str, ResolvedModel] = {}
        self._raw_profiles: Optional[dict] = None
        self._source_connections: Optional[Dict[str, str]] = None
        self._cache: Optional[DvtCache] = None

    def _resolve_models(self) -> None:
        """Resolve execution paths for all models in the manifest."""
        if self._resolutions:
            return

        # Read profiles.yml
        profiles_dir = (
            getattr(self.args, "PROFILES_DIR", None) or default_profiles_dir()
        )
        try:
            self._raw_profiles = read_profiles_yml(profiles_dir)
        except Exception as e:
            logger.warning(f"DVT: Could not read profiles.yml: {e}")
            self._raw_profiles = {}

        # Read source connections
        project_dir = getattr(self.args, "PROJECT_DIR", None) or "."
        self._source_connections = load_source_connections(project_dir)

        # Validate: DVT113 — source has connection: to same adapter type as default
        if self._source_connections and self._raw_profiles:
            errors = validate_source_connections(
                self._source_connections, self._raw_profiles, self.config.target_name
            )
            for err in errors:
                import sys

                print(f"\n  ⚠️  {err}\n", file=sys.stderr)

        # Resolve
        # default_target comes from profiles.yml "target:" field
        # self.config.target_name may differ if --target was passed
        profiles_default = None
        if self._raw_profiles:
            for pname, pdata in self._raw_profiles.items():
                if isinstance(pdata, dict) and "target" in pdata:
                    profiles_default = pdata["target"]
                    break
        default_target = profiles_default or self.config.target_name
        active_target = self.config.target_name
        cli_target = active_target if active_target != default_target else None

        # DVT007: Warn if --target changes adapter type
        if cli_target and self._raw_profiles:
            from dvt.sync.profiles_reader import extract_outputs

            outputs = extract_outputs(self._raw_profiles)
            output_map = {o["_name"]: o.get("type", "") for o in outputs}
            default_type = output_map.get(default_target, "")
            cli_type = output_map.get(cli_target, "")
            if default_type and cli_type and default_type != cli_type:
                import sys

                print(
                    f"\n  ⚠️  DVT007: --target '{cli_target}' uses '{cli_type}' adapter, "
                    f"but your default target uses '{default_type}'.\n"
                    f"     This will require you to migrate all {default_type} "
                    f"models to {cli_type} dialect.\n",
                    file=sys.stderr,
                )

        self._resolutions = resolve_all_models(
            self.manifest, default_target, self._source_connections, cli_target
        )

        # Count extraction models
        extraction_count = sum(
            1
            for r in self._resolutions.values()
            if r.execution_path
            in (
                ExecutionPath.SLING_DIRECT,
                ExecutionPath.DUCKDB_COMPUTE,
                ExecutionPath.NON_DEFAULT_PUSHDOWN,
            )
        )

        if extraction_count > 0:
            logger.info(f"DVT: {extraction_count} model(s) require extraction")

            # Initialize the DuckDB cache
            project_dir = getattr(self.args, "PROJECT_DIR", None) or "."
            self._cache = DvtCache(project_dir=project_dir)

            # --full-refresh: destroy cache
            full_refresh = getattr(self.args, "FULL_REFRESH", False)
            if full_refresh:
                logger.info("DVT: --full-refresh — destroying DuckDB cache")
                self._cache.destroy()
                # Recreate (destroy deleted the dir, cache will recreate on first use)
                self._cache = DvtCache(project_dir=project_dir)

    def get_runner_type(self, node) -> Optional[Type]:
        """Return DvtModelRunner for extraction models."""
        self._resolve_models()

        uid = getattr(node, "unique_id", None)
        if uid and uid in self._resolutions:
            resolution = self._resolutions[uid]
            if resolution.execution_path in (
                ExecutionPath.SLING_DIRECT,
                ExecutionPath.DUCKDB_COMPUTE,
                ExecutionPath.NON_DEFAULT_PUSHDOWN,
            ):
                return DvtModelRunner

        return super().get_runner_type(node)

    def get_runner(self, node):
        """Get runner and inject DVT context (resolution, profiles, cache)."""
        runner = super().get_runner(node)

        if isinstance(runner, DvtModelRunner):
            uid = getattr(node, "unique_id", None)
            if uid and uid in self._resolutions:
                runner.set_dvt_context(
                    resolution=self._resolutions[uid],
                    profiles=self._raw_profiles or {},
                    cache=self._cache,
                )

        return runner
