"""
Federation execution engine for DVT.

Executes cross-target queries using Spark:
1. Build view mappings and translate SQL to Spark SQL
2. Pre-fetch source schemas and run FederationOptimizer
3. Extract sources to staging (with column + predicate + LIMIT pushdown)
4. Register staged data as Spark temp views
5. Execute in Spark
6. Write results to target via Loader (bucket COPY or JDBC fallback)

Usage:
    from dvt.federation.engine import FederationEngine
    from dvt.federation.resolver import ResolvedExecution

    engine = FederationEngine(runtime_config, manifest)
    result = engine.execute(model, resolution, compiled_sql)
"""

import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from dbt_common.events.functions import fire_event

from dvt.config.user_config import (
    get_bucket_path,
    load_buckets_for_profile,
    load_computes_for_profile,
    load_profiles,
)
from dvt.federation.dialect_fallbacks import get_dialect_for_adapter
from dvt.federation.el_layer import ELLayer, SourceConfig, create_el_layer
from dvt.federation.extractors import get_extractor
from dvt.federation.federation_optimizer import (
    ExtractionPlan,
    FederationOptimizer,
    SourceInfo,
)
from dvt.federation.loaders import get_loader
from dvt.federation.loaders.base import LoadConfig, LoadResult
from dvt.federation.query_optimizer import PushableOperations, QueryOptimizer
from dvt.federation.resolver import ExecutionPath, ResolvedExecution
from dvt.federation.spark_manager import SparkManager


class FederationEngine:
    """Executes federated queries via Spark.

    The engine handles the full lifecycle of a federated query:
    1. EXTRACT: Pull data from source databases to staging bucket
    2. TRANSFORM: Execute SQL in Spark (with SQLGlot translation)
    3. LOAD: Write results to target database

    Each model gets an isolated namespace for temp views to prevent
    conflicts when running parallel models.
    """

    def __init__(
        self,
        runtime_config: Any,
        manifest: Any,
        on_progress: Optional[callable] = None,
    ):
        """Initialize the federation engine.

        Args:
            runtime_config: RuntimeConfig with profile info
            manifest: Parsed project manifest
            on_progress: Optional callback for progress messages
        """
        self.config = runtime_config
        self.manifest = manifest
        self.on_progress = on_progress or (lambda msg: None)
        self.query_optimizer = QueryOptimizer()
        self.federation_optimizer = FederationOptimizer()

        # Will be initialized on first execute
        self._profiles: Optional[Dict[str, Any]] = None

    def _log(self, message: str) -> None:
        """Log a progress message."""
        self.on_progress(message)

    def execute(
        self,
        model: Any,
        resolution: ResolvedExecution,
        compiled_sql: str,
    ) -> Dict[str, Any]:
        """Execute a model via Spark federation.

        IMPORTANT: compiled_sql has Jinja resolved but is in the
        model's target dialect (e.g., Snowflake SQL). We translate
        to Spark SQL using SQLGlot.

        For incremental models, {{ this }} in the compiled SQL resolves to
        the target table reference (e.g., "dvt_test"."my_model"). We register
        a local Delta staging copy of the model's own output as a Spark temp
        view, and rewrite {{ this }} references to point to it. This enables:
        - Filter-based: WHERE col > (SELECT MAX(col) FROM {{ this }})
        - Left-join-based: LEFT JOIN {{ this }} ON ... WHERE {{ this }}.id IS NULL

        Args:
            model: ModelNode to execute
            resolution: Resolved execution details
            compiled_sql: Compiled SQL (Jinja resolved)

        Returns:
            Dict with execution results:
            - success: bool
            - row_count: int
            - execution_time: float
            - message: str
            - error: Optional[str]
        """
        start_time = time.time()
        model_name = model.name
        view_prefix = self._get_view_prefix(model)

        self._log(f"Starting federation for {model_name}")
        self._log(f"  Target: {resolution.target}")
        self._log(f"  Upstream targets: {resolution.upstream_targets}")

        try:
            # 1. Get SparkManager instance (already initialized by RunTask)
            spark_manager = SparkManager.get_instance()
            spark = spark_manager.get_or_create_session(f"DVT-{model_name}")

            # 2. Create EL layer for staging
            el_layer = self._create_el_layer(resolution)
            if el_layer is None:
                raise RuntimeError(
                    f"Could not create EL layer for bucket '{resolution.bucket}'"
                )

            # 2a. Handle --full-refresh for model staging
            full_refresh = getattr(
                getattr(self.config, "args", None), "FULL_REFRESH", False
            )
            if full_refresh:
                self._clear_model_staging(el_layer, model)

            # 3. Determine the SQL dialect of the compiled SQL.
            # DVT: With target-aware compilation, the compiled SQL is in the
            # TARGET adapter's dialect (e.g., databricks backticks when the
            # model targets databricks). We must parse using that dialect.
            source_dialect = self._get_dialect_for_target(resolution.target)

            # 4. Build source mappings (source_id -> alias in SQL)
            source_mappings = self._build_source_mappings(
                model, compiled_sql, sql_dialect=source_dialect
            )

            # 5. Build view mappings from manifest metadata (no data needed).
            # This computes the {original_table_ref: temp_view_name} dict
            # needed for Spark SQL translation, without reading staged data.
            view_mappings = self._build_view_mappings(
                model=model,
                source_mappings=source_mappings,
                view_prefix=view_prefix,
            )

            # 5a. Register model's own Delta staging as {{ this }} temp view
            # and add {{ this }} entries to view_mappings BEFORE Spark SQL
            # translation, so self-references get rewritten to temp views.
            self._register_model_self_view(
                spark=spark,
                model=model,
                el_layer=el_layer,
                view_prefix=view_prefix,
                view_mappings=view_mappings,
            )

            # 6. Translate SQL: model dialect -> Spark SQL (BEFORE extraction).
            # This only needs view_mappings — not extracted data.
            spark_sql = self._translate_to_spark(
                compiled_sql=compiled_sql,
                source_dialect=source_dialect,
                view_mappings=view_mappings,
            )

            # 7. Pre-fetch source schemas for the FederationOptimizer.
            # Lightweight metadata queries (~100ms per source, no data transfer).
            source_info = self._fetch_source_schemas(
                model=model,
                resolution=resolution,
                source_mappings=source_mappings,
                view_prefix=view_prefix,
            )

            # 8. FederationOptimizer: decompose Spark SQL into per-source
            # extraction plans with column projection, predicate pushdown,
            # and LIMIT — all transpiled to each source's native dialect.
            extraction_plans = self.federation_optimizer.optimize(
                spark_sql=spark_sql,
                source_info=source_info,
            )

            if extraction_plans:
                optimized_sources = [
                    sid for sid, p in extraction_plans.items() if p.columns
                ]
                self._log(
                    f"  Optimizer: column pushdown on {len(optimized_sources)}/{len(extraction_plans)} sources, "
                    f"{sum(len(p.predicates) for p in extraction_plans.values())} predicates pushed"
                )

            # 9. Extract sources to staging bucket (with optimized pushdown)
            self._log(f"Extracting {len(source_mappings)} sources to staging...")
            extraction_result = self._extract_sources(
                model=model,
                resolution=resolution,
                el_layer=el_layer,
                extraction_plans=extraction_plans,
            )

            if not extraction_result.get("success", False):
                raise RuntimeError(
                    f"Source extraction failed: {extraction_result.get('errors', {})}"
                )

            self._log(
                f"  Extracted {extraction_result.get('total_rows', 0):,} rows "
                f"from {extraction_result.get('sources_extracted', 0)} sources"
            )

            # 9a. Extract ref model data (for transitive deps through ephemerals).
            # Models referenced by ephemeral CTEs live in external databases
            # and need to be extracted to staging before Spark can use them.
            self._extract_ref_models(
                model=model,
                el_layer=el_layer,
            )

            # 10. Register staged data as Spark temp views
            self._register_temp_views(
                spark=spark,
                model=model,
                el_layer=el_layer,
                view_prefix=view_prefix,
                source_mappings=source_mappings,
                view_mappings=view_mappings,
            )

            self._log(f"Executing Spark SQL...")

            # 11. Execute in Spark
            result_df = spark.sql(spark_sql)

            # 12. Write to target
            load_result = self._write_to_target(
                df=result_df,
                model=model,
                resolution=resolution,
            )

            if not load_result.success:
                raise RuntimeError(f"Load failed: {load_result.error}")

            # 13. Save model output to Delta staging for future {{ this }} resolution
            self._save_model_staging(
                spark=spark,
                el_layer=el_layer,
                model=model,
                result_df=result_df,
                resolution=resolution,
            )

            elapsed = time.time() - start_time
            self._log(
                f"Federation complete: {load_result.row_count:,} rows "
                f"via {load_result.load_method} in {elapsed:.1f}s"
            )

            return {
                "success": True,
                "row_count": load_result.row_count,
                "execution_time": elapsed,
                "message": f"Federation: {load_result.row_count:,} rows via {load_result.load_method}",
                "load_method": load_result.load_method,
            }

        except Exception as e:
            elapsed = time.time() - start_time
            error_msg = str(e)
            self._log(f"Federation failed: {error_msg}")

            return {
                "success": False,
                "row_count": 0,
                "execution_time": elapsed,
                "message": f"Federation failed: {error_msg}",
                "error": error_msg,
            }

        finally:
            # Cleanup temp views for this model
            try:
                spark = SparkManager.get_instance().get_or_create_session()
                self._cleanup_temp_views(spark, view_prefix)
            except Exception:
                pass  # Ignore cleanup errors

    def _get_view_prefix(self, model: Any) -> str:
        """Generate unique prefix for temp views to avoid collisions.

        Args:
            model: ModelNode

        Returns:
            Unique prefix string
        """
        # Use first 8 chars of unique_id hash for brevity
        import hashlib

        model_hash = hashlib.md5(model.unique_id.encode()).hexdigest()[:8]
        return f"_dvt_{model_hash}_"

    def _create_el_layer(self, resolution: ResolvedExecution) -> Optional[ELLayer]:
        """Create EL layer for source extraction.

        Args:
            resolution: Resolved execution details

        Returns:
            ELLayer instance or None if bucket not configured
        """
        profile_name = self._get_profile_name()
        profiles_dir = self._get_profiles_dir()

        return create_el_layer(
            profile_name=profile_name,
            profiles_dir=profiles_dir,
            on_progress=self._log,
        )

    def _build_view_mappings(
        self,
        model: Any,
        source_mappings: Dict[str, str],
        view_prefix: str,
    ) -> Dict[str, str]:
        """Build view mappings from manifest metadata (no data needed).

        Computes the {original_table_ref: temp_view_name} dict that
        _translate_to_spark() needs, using only manifest source definitions.
        Does NOT read or register staged data — that happens later in
        _register_temp_views().

        Args:
            model: ModelNode
            source_mappings: source_id -> alias mapping
            view_prefix: Unique prefix for temp view names

        Returns:
            Dict mapping original table reference to temp view name
        """
        view_mappings: Dict[str, str] = {}

        if not hasattr(model, "depends_on") or not model.depends_on:
            return view_mappings

        nodes = getattr(model.depends_on, "nodes", []) or []

        for dep_id in nodes:
            if dep_id.startswith("source."):
                source = self.manifest.sources.get(dep_id)
                if not source:
                    continue

                alias = source_mappings.get(dep_id, source.name)
                view_name = f"{view_prefix}{alias}"

                database = getattr(source, "database", None) or ""
                schema = source.schema or ""
                table = source.name

                # 3-part: db.schema.table
                if database and schema:
                    view_mappings[f"{database}.{schema}.{table}"] = view_name
                # 2-part: schema.table
                if schema:
                    view_mappings[f"{schema}.{table}"] = view_name
                # 1-part: just table name
                view_mappings[table] = view_name
                # Also map the alias if different
                if alias and alias != table:
                    view_mappings[alias] = view_name

            elif dep_id.startswith("model."):
                # Handle ref() model dependencies.
                # When a federation model references another model via ref(),
                # that model's output needs to be read from Delta staging
                # (not from the original database). Register a view mapping
                # so _translate_to_spark() rewrites the table reference.
                #
                # Ephemeral models are inlined as CTEs, but their OWN
                # dependencies (non-ephemeral models) still appear as table
                # references inside the CTE SQL. We must recursively resolve
                # through ephemeral models to find all non-ephemeral refs.
                resolved_refs = self._resolve_model_refs_through_ephemerals(dep_id)
                for ref_node in resolved_refs:
                    ref_name = ref_node.name
                    view_name = f"{view_prefix}{ref_name}"

                    database = getattr(ref_node, "database", None) or ""
                    schema = getattr(ref_node, "schema", None) or ""

                    # 3-part: db.schema.table
                    if database and schema:
                        view_mappings[f"{database}.{schema}.{ref_name}"] = view_name
                    # 2-part: schema.table
                    if schema:
                        view_mappings[f"{schema}.{ref_name}"] = view_name
                    # 1-part: just table name
                    view_mappings[ref_name] = view_name

        return view_mappings

    def _resolve_model_refs_through_ephemerals(
        self, dep_id: str, _visited: Optional[set] = None
    ) -> list:
        """Resolve a model dependency, recursing through ephemeral models.

        If dep_id points to an ephemeral model, recursively walk its
        depends_on.nodes to find all non-ephemeral model dependencies.
        Returns a list of non-ephemeral model nodes that need view mappings.

        Args:
            dep_id: Model unique_id (e.g., 'model.Coke_DB.some_model')
            _visited: Set of already-visited IDs to prevent infinite loops

        Returns:
            List of non-ephemeral ModelNode objects
        """
        if _visited is None:
            _visited = set()
        if dep_id in _visited:
            return []
        _visited.add(dep_id)

        ref_node = self.manifest.nodes.get(dep_id)
        if not ref_node:
            return []

        mat = getattr(getattr(ref_node, "config", None), "materialized", None)
        if mat != "ephemeral":
            # Non-ephemeral — this is an actual table/view we need to register
            return [ref_node]

        # Ephemeral model: recurse into its own dependencies
        results = []
        child_nodes = getattr(getattr(ref_node, "depends_on", None), "nodes", []) or []
        for child_id in child_nodes:
            if child_id.startswith("model."):
                results.extend(
                    self._resolve_model_refs_through_ephemerals(child_id, _visited)
                )
            # source. deps are handled separately — they appear in the
            # parent model's depends_on already or in the CTE SQL, and
            # the source extraction path handles them.
        return results

    def _fetch_source_schemas(
        self,
        model: Any,
        resolution: ResolvedExecution,
        source_mappings: Dict[str, str],
        view_prefix: str,
    ) -> Dict[str, SourceInfo]:
        """Pre-fetch source schemas for the FederationOptimizer.

        Makes lightweight metadata-only queries (~100ms per source) to get
        real column names from each source database. This information is
        needed by the optimizer to:
        - Determine which columns to extract per source
        - Resolve column name casing (sqlglot may lowercase)
        - Expand SELECT * into actual column names

        Args:
            model: ModelNode
            resolution: Resolved execution details
            source_mappings: source_id -> alias mapping
            view_prefix: Unique prefix for temp view names

        Returns:
            Dict mapping source_id to SourceInfo
        """
        source_info: Dict[str, SourceInfo] = {}

        if not hasattr(model, "depends_on") or not model.depends_on:
            return source_info

        nodes = getattr(model.depends_on, "nodes", []) or []

        for dep_id in nodes:
            if not dep_id.startswith("source."):
                continue

            source = self.manifest.sources.get(dep_id)
            if not source:
                continue

            # Get source's connection config
            connection_name = None
            if hasattr(source, "config") and source.config:
                connection_name = getattr(source.config, "connection", None)
            if not connection_name:
                connection_name = getattr(source, "connection", None)
            if not connection_name:
                connection_name = self._get_default_target()

            connection_config = self._get_connection_config(connection_name)
            if not connection_config:
                continue

            adapter_type = connection_config.get("type", "")
            alias = source_mappings.get(dep_id, source.name)
            view_name = f"{view_prefix}{alias}"

            # Pre-fetch column metadata via extractor (lightweight metadata query)
            try:
                extractor = get_extractor(
                    adapter_type=adapter_type,
                    connection=None,  # Lazy connection creation
                    dialect=adapter_type,
                    on_progress=self._log,
                    connection_config=connection_config,
                )
                columns_info = extractor.get_columns(source.schema or "", source.name)
                column_names = [c["name"] for c in columns_info]
            except Exception as e:
                self._log(
                    f"  Warning: Could not fetch schema for {dep_id}: {e}. "
                    f"Using SELECT * for this source."
                )
                column_names = []

            source_info[dep_id] = SourceInfo(
                source_id=dep_id,
                view_name=view_name,
                schema=source.schema or "",
                table=source.name,
                columns=column_names,
                dialect=adapter_type,
            )

        return source_info

    def _build_source_mappings(
        self,
        model: Any,
        compiled_sql: str,
        sql_dialect: Optional[str] = None,
    ) -> Dict[str, str]:
        """Build mapping from source_id to SQL alias.

        This analyzes the compiled SQL to find table aliases
        for each source reference.

        Args:
            model: ModelNode
            compiled_sql: Compiled SQL to analyze
            sql_dialect: SQLGlot dialect of the compiled SQL (e.g., "databricks")

        Returns:
            Dict mapping source_id to alias (e.g., "source.proj.orders" -> "o")
        """
        mappings = {}

        if not hasattr(model, "depends_on") or not model.depends_on:
            return mappings

        nodes = getattr(model.depends_on, "nodes", []) or []

        for dep_id in nodes:
            if dep_id.startswith("source."):
                source = self.manifest.sources.get(dep_id)
                if source:
                    # Try to find alias in SQL
                    # Default to table name if alias not found
                    table_name = source.name
                    schema_name = source.schema

                    alias = self._find_table_alias(
                        compiled_sql,
                        schema_name,
                        table_name,
                        sql_dialect=sql_dialect,
                    )

                    mappings[dep_id] = alias or table_name

        return mappings

    def _find_table_alias(
        self,
        sql: str,
        schema: str,
        table: str,
        sql_dialect: Optional[str] = None,
    ) -> Optional[str]:
        """Find table alias in SQL using SQLGlot.

        Args:
            sql: SQL to analyze
            schema: Schema name
            table: Table name
            sql_dialect: SQLGlot dialect for parsing (e.g., "databricks")

        Returns:
            Alias if found, None otherwise
        """
        try:
            parsed = sqlglot.parse_one(sql, read=sql_dialect)

            for tbl in parsed.find_all(exp.Table):
                tbl_name = tbl.name
                tbl_db = tbl.db

                # Match by table name (with optional schema)
                if tbl_name.lower() == table.lower():
                    if tbl_db and tbl_db.lower() != schema.lower():
                        continue  # Schema doesn't match

                    # Found the table, get alias
                    alias = tbl.alias
                    if alias:
                        return alias

                    # No alias, return table name
                    return tbl_name

        except Exception:
            pass

        return None

    def _extract_sources(
        self,
        model: Any,
        resolution: ResolvedExecution,
        el_layer: ELLayer,
        extraction_plans: Dict[str, ExtractionPlan],
    ) -> Dict[str, Any]:
        """Extract sources to staging bucket with optimized pushdown.

        Uses ExtractionPlans from the FederationOptimizer, which provide:
        - Column projection (only columns the query needs)
        - Predicate pushdown (transpiled to source dialect)
        - LIMIT pushdown (transpiled to source syntax)
        - Complete extraction SQL (ready to execute on source)

        For parallel safety, the EL layer handles union-of-columns logic:
        if staging already exists but is missing columns needed by this model,
        it re-extracts with the union of existing + new columns.

        Args:
            model: ModelNode
            resolution: Resolved execution details
            el_layer: EL layer for extraction
            extraction_plans: Per-source extraction plans from optimizer

        Returns:
            Dict with extraction results
        """
        sources = []

        if not hasattr(model, "depends_on") or not model.depends_on:
            return {"success": True, "sources_extracted": 0, "total_rows": 0}

        nodes = getattr(model.depends_on, "nodes", []) or []

        for dep_id in nodes:
            if not dep_id.startswith("source."):
                continue

            source = self.manifest.sources.get(dep_id)
            if not source:
                continue

            # Get source's connection (stored in source.config.connection)
            connection_name = None
            if hasattr(source, "config") and source.config:
                connection_name = getattr(source.config, "connection", None)
            # Fallback to top-level for backwards compatibility
            if not connection_name:
                connection_name = getattr(source, "connection", None)
            if not connection_name:
                # Use default target
                connection_name = self._get_default_target()

            connection_config = self._get_connection_config(connection_name)
            if not connection_config:
                self._log(f"  Warning: No connection config for {connection_name}")
                continue

            # Get extraction plan from the FederationOptimizer
            plan = extraction_plans.get(dep_id)

            sources.append(
                SourceConfig(
                    source_name=dep_id,
                    adapter_type=connection_config.get("type", ""),
                    schema=source.schema,
                    table=source.name,
                    connection=None,  # Will be created by extractor
                    connection_config=connection_config,
                    columns=plan.columns if plan else None,
                    predicates=plan.predicates if plan else None,
                    limit=plan.limit if plan else None,
                    extraction_sql=plan.extraction_sql if plan else None,
                )
            )

        if not sources:
            return {"success": True, "sources_extracted": 0, "total_rows": 0}

        # Extract all sources (full_refresh=True forces re-extraction)
        full_refresh = getattr(
            getattr(self.config, "args", None), "FULL_REFRESH", False
        )
        result = el_layer.extract_sources_parallel(sources, full_refresh=full_refresh)

        return {
            "success": result.success,
            "sources_extracted": result.sources_extracted,
            "sources_skipped": result.sources_skipped,
            "sources_failed": result.sources_failed,
            "total_rows": result.total_rows,
            "errors": result.errors,
        }

    def _extract_ref_models(
        self,
        model: Any,
        el_layer: ELLayer,
    ) -> None:
        """Extract ref model data for transitive dependencies through ephemerals.

        When a federation model references an ephemeral model that itself refs
        a non-ephemeral model, the non-ephemeral model's data needs to be in
        staging so it can be registered as a Spark temp view.

        Unlike source extraction (which is always needed), ref model extraction
        is only needed when the ref model's data isn't already in staging.
        Models that previously ran on the federation path already have staging.
        Models that run via adapter pushdown (standard path) don't — we need
        to extract their data via JDBC.

        Args:
            model: The federation model being executed
            el_layer: EL layer for staging operations
        """
        if not hasattr(model, "depends_on") or not model.depends_on:
            return

        nodes = getattr(model.depends_on, "nodes", []) or []

        for dep_id in nodes:
            if not dep_id.startswith("model."):
                continue

            # Resolve through ephemerals to find actual table-backed models
            resolved_refs = self._resolve_model_refs_through_ephemerals(dep_id)

            for ref_node in resolved_refs:
                model_staging_id = self._get_model_staging_id(ref_node)

                # Skip if staging already exists (model ran on federation path,
                # or was extracted by a previous model in this run)
                if el_layer.staging_exists(model_staging_id):
                    continue

                # Need to extract from the model's database
                ref_target = getattr(getattr(ref_node, "config", None), "target", None)
                if not ref_target:
                    ref_target = self._get_default_target()

                connection_config = self._get_connection_config(ref_target)
                if not connection_config:
                    self._log(
                        f"  Warning: No connection for ref model {ref_node.name} "
                        f"target {ref_target}"
                    )
                    continue

                ref_schema = getattr(ref_node, "schema", None) or ""
                ref_name = ref_node.name

                self._log(
                    f"  Extracting ref model {ref_name} from {ref_target} "
                    f"({ref_schema}.{ref_name})..."
                )

                # Build a SourceConfig to use the existing extraction pipeline
                source_cfg = SourceConfig(
                    source_name=model_staging_id,
                    adapter_type=connection_config.get("type", ""),
                    schema=ref_schema,
                    table=ref_name,
                    connection=None,
                    connection_config=connection_config,
                )

                full_refresh = getattr(
                    getattr(self.config, "args", None), "FULL_REFRESH", False
                )
                result = el_layer.extract_sources_parallel(
                    [source_cfg], full_refresh=full_refresh
                )

                if not result.success:
                    self._log(
                        f"  Warning: Failed to extract ref model {ref_name}: "
                        f"{result.errors}"
                    )

    def _register_temp_views(
        self,
        spark: Any,
        model: Any,
        el_layer: ELLayer,
        view_prefix: str,
        source_mappings: Dict[str, str],
        view_mappings: Optional[Dict[str, str]] = None,
    ) -> Dict[str, str]:
        """Register staged data as Spark temp views.

        Reads extracted Delta/Parquet staging and registers each as a Spark
        temp view. The view_mappings dict (original_ref -> view_name) was
        already computed by _build_view_mappings() — this method only does
        the Spark read + createOrReplaceTempView part.

        Args:
            spark: SparkSession
            model: ModelNode
            el_layer: EL layer with staging paths
            view_prefix: Unique prefix for view names
            source_mappings: source_id -> SQL alias mappings
            view_mappings: Pre-computed view mappings (optional, for backward compat)

        Returns:
            Dict mapping original table reference to temp view name
        """
        if view_mappings is None:
            view_mappings = {}

        if not hasattr(model, "depends_on") or not model.depends_on:
            return view_mappings

        nodes = getattr(model.depends_on, "nodes", []) or []

        for dep_id in nodes:
            if dep_id.startswith("source."):
                source = self.manifest.sources.get(dep_id)
                if not source:
                    continue

                # Get staging path
                staging_path = el_layer.get_staging_path(dep_id)
                if not el_layer.staging_exists(dep_id):
                    self._log(f"  Warning: No staging data for {dep_id}")
                    continue

                # Read staging data (auto-detect Delta vs legacy Parquet)
                staging_path_obj = Path(str(staging_path))
                try:
                    if (
                        staging_path_obj.is_dir()
                        and (staging_path_obj / "_delta_log").is_dir()
                    ):
                        df = spark.read.format("delta").load(str(staging_path))
                    else:
                        df = spark.read.parquet(str(staging_path))
                except Exception as e:
                    self._log(f"  Warning: Could not read staging for {dep_id}: {e}")
                    continue

                # Create temp view with unique prefix
                alias = source_mappings.get(dep_id, source.name)
                view_name = f"{view_prefix}{alias}"
                df.createOrReplaceTempView(view_name)

                self._log(f"  Registered temp view: {view_name}")

            elif dep_id.startswith("model."):
                # Handle ref() model dependencies — read from model staging.
                # Recurse through ephemeral models to find actual tables.
                resolved_refs = self._resolve_model_refs_through_ephemerals(dep_id)
                for ref_node in resolved_refs:
                    # Model staging uses model unique_id as the staging key
                    model_staging_id = self._get_model_staging_id(ref_node)
                    staging_path = el_layer.get_staging_path(model_staging_id)
                    if not el_layer.staging_exists(model_staging_id):
                        self._log(
                            f"  Warning: No staging data for ref model "
                            f"{ref_node.name} (run the upstream model first)"
                        )
                        continue

                    staging_path_obj = Path(str(staging_path))
                    try:
                        if (
                            staging_path_obj.is_dir()
                            and (staging_path_obj / "_delta_log").is_dir()
                        ):
                            df = spark.read.format("delta").load(str(staging_path))
                        else:
                            df = spark.read.parquet(str(staging_path))
                    except Exception as e:
                        self._log(
                            f"  Warning: Could not read staging for ref "
                            f"{ref_node.name}: {e}"
                        )
                        continue

                    view_name = f"{view_prefix}{ref_node.name}"
                    df.createOrReplaceTempView(view_name)
                    self._log(f"  Registered temp view: {view_name} (ref model)")

        return view_mappings

    # =========================================================================
    # Model Self-Staging for {{ this }} Resolution
    # =========================================================================

    def _get_model_staging_id(self, model: Any) -> str:
        """Get the staging identifier for a model's own Delta table.

        Uses the model's unique_id to create a stable, unique staging path
        that persists across runs for incremental models.

        Args:
            model: ModelNode

        Returns:
            Staging identifier like 'model.project.my_model'
        """
        return model.unique_id

    def _register_model_self_view(
        self,
        spark: Any,
        model: Any,
        el_layer: ELLayer,
        view_prefix: str,
        view_mappings: Dict[str, str],
    ) -> None:
        """Register the model's own Delta staging as a temp view for {{ this }}.

        When an incremental model references {{ this }}, dbt resolves it to
        the target table reference (e.g., "dvt_test"."my_model"). On the
        federation path, Spark can't access the remote target — so we register
        a local Delta staging copy of the model's previous output as a temp view
        and add it to view_mappings so _translate_to_spark() rewrites the reference.

        This enables both incremental patterns:
        - Filter: WHERE col > (SELECT MAX(col) FROM {{ this }})
        - Anti-join: LEFT JOIN {{ this }} t ON ... WHERE t.id IS NULL

        On first run (no Delta staging exists), this is a no-op because
        is_incremental() returned False during compilation, so the compiled SQL
        does not contain {{ this }} references.

        Args:
            spark: SparkSession
            model: ModelNode
            el_layer: EL layer with staging paths
            view_prefix: Unique prefix for temp view names
            view_mappings: Dict to update with {{ this }} mapping (mutated in place)
        """
        model_staging_id = self._get_model_staging_id(model)

        # Check if model has Delta staging from a previous run
        if not el_layer.staging_exists(model_staging_id):
            return  # First run — no staging, is_incremental() was False

        staging_path = el_layer.get_staging_path(model_staging_id)
        staging_path_obj = Path(str(staging_path))

        # Read Delta staging (or legacy Parquet fallback).
        # Wrap in try/except: corrupted/empty Delta logs should not fail
        # the entire federation — just skip self-view registration.
        try:
            if staging_path_obj.is_dir() and (staging_path_obj / "_delta_log").is_dir():
                df = spark.read.format("delta").load(str(staging_path))
            else:
                df = spark.read.parquet(str(staging_path))
        except Exception as e:
            self._log(
                f"  Warning: Could not read model staging for {{{{ this }}}} "
                f"({model_staging_id}): {e}. Skipping self-view registration."
            )
            return

        # Register as temp view
        view_name = f"{view_prefix}_this"
        df.createOrReplaceTempView(view_name)

        # Add all possible {{ this }} reference forms to view_mappings.
        # After Jinja resolution, {{ this }} becomes a quoted table reference
        # like "dvt_test"."my_model" or `dvt_test`.`my_model`. SQLGlot parses
        # these into exp.Table nodes with db=schema, name=table_name.
        schema = model.schema or ""
        table = model.name
        database = getattr(model, "database", None) or ""

        # 3-part: database.schema.table
        if database and schema:
            view_mappings[f"{database}.{schema}.{table}"] = view_name
        # 2-part: schema.table (most common for {{ this }})
        if schema:
            view_mappings[f"{schema}.{table}"] = view_name
        # 1-part: just table name
        view_mappings[table] = view_name

        self._log(f"  Registered {{ this }} view: {view_name} ({df.count()} rows)")

    def _save_model_staging(
        self,
        spark: Any,
        el_layer: ELLayer,
        model: Any,
        result_df: Any,
        resolution: ResolvedExecution,
    ) -> None:
        """Save the model's result DataFrame to Delta staging.

        After a successful load to the target database, we save a local Delta
        copy of the output. On subsequent incremental runs, this Delta table is
        registered as the {{ this }} temp view so Spark can resolve self-references
        without querying the remote target.

        Delta write mode mirrors the target behavior:
        - table/view: overwrite (target rebuilt every run)
        - incremental + append (default): append (rows accumulate on target)
        - incremental + merge: overwrite (target upserts; staging must not
          accumulate duplicate keys)
        - incremental + delete+insert: overwrite (target replaces matched rows)

        Args:
            spark: SparkSession
            el_layer: EL layer for staging paths
            model: ModelNode
            result_df: PySpark DataFrame with the model's output
            resolution: Resolved execution details
        """
        model_staging_id = self._get_model_staging_id(model)
        staging_path = el_layer.state_manager.bucket_path / f"{model_staging_id}.delta"

        mat = resolution.coerced_materialization or resolution.original_materialization

        # Delta write mode must mirror what happens on the target:
        #   - table/view: overwrite (target is rebuilt every run)
        #   - incremental + append (default): append (rows accumulate)
        #   - incremental + merge: overwrite (target upserts matched rows;
        #     staging must reflect the result, not accumulate duplicates)
        #   - incremental + delete+insert: overwrite (same reasoning as merge)
        if mat == "incremental":
            strategy = getattr(
                getattr(model, "config", None), "incremental_strategy", None
            )
            if strategy in ("merge", "delete+insert"):
                delta_mode = "overwrite"
            else:
                # Default append strategy — staging accumulates like the target
                delta_mode = "append"
        else:
            delta_mode = "overwrite"

        try:
            writer = (
                result_df.write.format("delta")
                .mode(delta_mode)
                .option("delta.columnMapping.mode", "name")
                .option("delta.minReaderVersion", "2")
                .option("delta.minWriterVersion", "5")
                .option("mergeSchema", "true")
            )

            # Apply user Delta table properties from computes.yml delta: section
            if SparkManager.is_initialized():
                for (
                    key,
                    value,
                ) in SparkManager.get_instance().delta_table_properties.items():
                    writer = writer.option(key, value)

            writer.save(str(staging_path))
        except Exception as e:
            # Non-fatal: model staging is an optimization for future incremental runs.
            # The current run already loaded to the target successfully.
            self._log(f"  Warning: Could not save model staging for {model.name}: {e}")

    def _clear_model_staging(self, el_layer: ELLayer, model: Any) -> None:
        """Clear the model's Delta staging on --full-refresh.

        When full_refresh is requested, the model's Delta staging must be cleared
        so that is_incremental() returns False (target table is rebuilt) and the
        next run starts fresh.

        Args:
            el_layer: EL layer for staging management
            model: ModelNode
        """
        model_staging_id = self._get_model_staging_id(model)
        el_layer.state_manager.clear_staging(model_staging_id)

    def _translate_to_spark(
        self,
        compiled_sql: str,
        source_dialect: str,
        view_mappings: Dict[str, str],
    ) -> str:
        """Translate compiled SQL to Spark SQL using SQLGlot.

        Args:
            compiled_sql: Compiled SQL in model's target dialect
            source_dialect: Source dialect name
            view_mappings: Dict mapping original refs to temp view names

        Returns:
            Spark SQL string
        """
        sqlglot_dialect = get_dialect_for_adapter(source_dialect)

        # Databricks/Spark dialects treat double-quoted strings as STRING
        # literals, not identifiers.  But DVT's compiled SQL always uses
        # double-quoted identifiers (ANSI/Postgres style) because column
        # names with spaces originate from source tables defined in SQL
        # files (e.g., c."Customer Code").  Parsing with the Databricks
        # dialect misinterprets these as string literals, producing
        # c.'Customer Code' instead of c.`Customer Code` in the output.
        # Fix: parse with 'postgres' dialect which treats double quotes
        # as identifiers, then transpile to Spark.
        if sqlglot_dialect in ("databricks", "spark"):
            sqlglot_dialect = "postgres"

        try:
            parsed = sqlglot.parse_one(compiled_sql, read=sqlglot_dialect)
        except ParseError as e:
            # If parsing fails, try to manually replace table refs
            return self._manual_table_replace(compiled_sql, view_mappings)

        # Rewrite table references to temp view names
        for table in parsed.find_all(exp.Table):
            # Build original references at different levels
            # SQLGlot uses: table.catalog for db, table.db for schema, table.name for table
            catalog = table.catalog or ""
            schema = table.db or ""
            name = table.name or ""

            # Try to match in order: 3-part, 2-part, 1-part
            original_ref = None

            # 3-part: catalog.schema.table
            if catalog and schema and name:
                full_ref_3 = f"{catalog}.{schema}.{name}"
                if full_ref_3 in view_mappings:
                    original_ref = full_ref_3

            # 2-part: schema.table
            if not original_ref and schema and name:
                full_ref_2 = f"{schema}.{name}"
                if full_ref_2 in view_mappings:
                    original_ref = full_ref_2

            # 1-part: just table name
            if not original_ref and name in view_mappings:
                original_ref = name

            # Check if we have a mapping for this table
            if original_ref and original_ref in view_mappings:
                view_name = view_mappings[original_ref]
                table.set("this", exp.Identifier(this=view_name))
                table.set("db", None)  # Remove schema prefix
                table.set("catalog", None)  # Remove catalog

        # Fix nested WITH clauses and duplicate CTE names.
        #
        # When dbt inlines ephemeral models, they may contain their own CTEs
        # (WITH source AS (...), renamed AS (...)). Postgres allows nested
        # WITH clauses and later CTEs to shadow earlier ones. Spark SQL
        # rejects both — no nested WITH and no duplicate CTE names.
        #
        # Strategy:
        # 1. Flatten: extract inner WITH clauses from CTEs, placing the inner
        #    CTEs before the parent CTE in the outer WITH.
        # 2. Deduplicate: rename duplicate CTE aliases and rewrite references.
        with_clause = parsed.find(exp.With)
        if with_clause:
            # Step 1: Flatten nested WITH clauses.
            # A CTE body may itself contain a WITH clause (e.g., ephemeral
            # models). Spark can't handle nested WITHs, so we extract the
            # inner CTEs and place them before the parent CTE.
            changed = True
            while changed:
                changed = False
                direct_ctes = list(with_clause.expressions)
                new_expressions = []
                for cte in direct_ctes:
                    if not isinstance(cte, exp.CTE):
                        new_expressions.append(cte)
                        continue
                    # Check if this CTE's body contains a nested WITH
                    cte_body = cte.this  # The SELECT inside the CTE
                    inner_with = cte_body.find(exp.With) if cte_body else None
                    if inner_with and inner_with is not with_clause:
                        # Extract inner CTEs before this CTE
                        for inner_cte in list(inner_with.expressions):
                            if isinstance(inner_cte, exp.CTE):
                                new_expressions.append(inner_cte)
                        # Remove the inner WITH from the CTE body,
                        # keeping only the final SELECT
                        inner_with.pop()
                        changed = True
                    new_expressions.append(cte)
                with_clause.set("expressions", new_expressions)

            # Step 2: Deduplicate CTE names (only direct children).
            direct_ctes = [c for c in with_clause.expressions if isinstance(c, exp.CTE)]
            seen_names: dict = {}  # name -> count
            rename_map: dict = {}  # cte_id -> (old_name, new_name)

            for cte in direct_ctes:
                alias_node = cte.args.get("alias")
                if not alias_node:
                    continue
                cte_name = (
                    alias_node.this
                    if isinstance(alias_node.this, str)
                    else alias_node.name
                )
                if not cte_name:
                    continue

                if cte_name in seen_names:
                    seen_names[cte_name] += 1
                    new_name = f"{cte_name}__dvt_{seen_names[cte_name]}"
                    rename_map[id(cte)] = (cte_name, new_name)
                    alias_node.set("this", exp.Identifier(this=new_name))
                else:
                    seen_names[cte_name] = 1

            # Step 3: Rewrite references to renamed CTEs.
            if rename_map:
                renamed_positions = {}
                for i, cte in enumerate(direct_ctes):
                    if id(cte) in rename_map:
                        renamed_positions[i] = rename_map[id(cte)]

                for pos, (old_name, new_name) in renamed_positions.items():
                    # Rewrite in CTEs that come AFTER the renamed one
                    for later_cte in direct_ctes[pos + 1 :]:
                        for table in later_cte.find_all(exp.Table):
                            if (
                                table.name == old_name
                                and not table.db
                                and not table.catalog
                            ):
                                table.set("this", exp.Identifier(this=new_name))

                    # Rewrite in the main SELECT body (outside CTEs)
                    parent_select = with_clause.parent
                    if parent_select:
                        for table in parent_select.find_all(exp.Table):
                            in_cte = False
                            node = table.parent
                            while node is not None:
                                if isinstance(node, exp.CTE):
                                    in_cte = True
                                    break
                                node = getattr(node, "parent", None)
                            if (
                                not in_cte
                                and table.name == old_name
                                and not table.db
                                and not table.catalog
                            ):
                                table.set("this", exp.Identifier(this=new_name))

        # Fix SQLGlot Snowflake→Spark transpilation bug:
        # SQLGlot converts NOT IN (subquery) to <> ALL (subquery) when parsing
        # Snowflake dialect, but Spark SQL doesn't support <> ALL syntax.
        # Transform it back to NOT ... IN (subquery).
        def _fix_neq_all(node: exp.Expression) -> exp.Expression:
            if isinstance(node, exp.NEQ):
                right = node.args.get("expression")
                if isinstance(right, exp.All):
                    subquery = right.this
                    if isinstance(subquery, exp.Select):
                        subquery = exp.Subquery(this=subquery)
                    return exp.Not(this=exp.In(this=node.this, query=subquery))
            return node

        parsed = parsed.transform(_fix_neq_all)

        # Transpile to Spark SQL
        return parsed.sql(dialect="spark")

    def _manual_table_replace(
        self,
        sql: str,
        view_mappings: Dict[str, str],
    ) -> str:
        """Manually replace table references when SQLGlot fails.

        This is a fallback for complex SQL that SQLGlot can't parse.

        Args:
            sql: Original SQL
            view_mappings: Dict mapping original refs to temp view names

        Returns:
            SQL with table references replaced
        """
        result = sql

        # Sort by length (longest first) to avoid partial matches
        sorted_mappings = sorted(
            view_mappings.items(), key=lambda x: len(x[0]), reverse=True
        )

        for original, view_name in sorted_mappings:
            # Replace table references (case insensitive)
            import re

            # Match table reference with word boundaries
            pattern = rf"\b{re.escape(original)}\b"
            result = re.sub(pattern, view_name, result, flags=re.IGNORECASE)

        return result

    def _resolve_target_table_name(
        self,
        model: Any,
        resolution: ResolvedExecution,
        target_config: Dict[str, Any],
    ) -> str:
        """Resolve the fully-qualified table name for writing to the target.

        When --target overrides the model's configured target, the model's
        schema/database (set at parse time from the default target) may not
        match the override target. This method resolves the correct
        schema and database from the override target's credentials.

        Priority for schema:
        1. Model's explicit schema config (user set config(schema='...'))
        2. Override target's schema from profiles.yml
        3. model.schema (parse-time default)

        Priority for database/catalog:
        1. Override target's catalog/database from profiles.yml
        2. model.database (parse-time default)

        Args:
            model: ModelNode
            resolution: Resolved execution details
            target_config: Connection config dict for the target

        Returns:
            Fully-qualified table name (schema.table or catalog.schema.table)
        """
        adapter_type = target_config.get("type", "")

        # Determine if --target is overriding the model's own target
        model_config_target = getattr(getattr(model, "config", None), "target", None)
        default_target = self._get_default_target()
        model_own_target = model_config_target or default_target
        is_target_override = resolution.target != model_own_target

        if is_target_override:
            # Check if the model has an explicit custom schema config.
            # If the model defines config(schema='my_schema'), respect it
            # even on the override target. Otherwise use the target's schema.
            model_has_custom_schema = getattr(
                getattr(model, "config", None), "schema", None
            )

            if model_has_custom_schema:
                effective_schema = model.schema
            else:
                effective_schema = target_config.get("schema") or model.schema

            # Database/catalog: always use override target's value
            effective_database = (
                target_config.get("catalog")
                or target_config.get("database")
                or getattr(model, "database", None)
                or ""
            )
        else:
            # model.schema is set at parse time from the DEFAULT target, NOT
            # from the model's config.target. When a model specifies
            # config(target='mssql_docker') but the profile default is
            # pg_docker, model.schema will be 'public' (Postgres) instead of
            # 'dbo' (MSSQL). Check if the model has an explicit custom schema;
            # if not, use the actual target config's schema.
            model_has_custom_schema = getattr(
                getattr(model, "config", None), "schema", None
            )
            if model_has_custom_schema:
                effective_schema = model.schema
            elif model_config_target and model_config_target != default_target:
                # Model targets a different engine than the default —
                # model.schema was parsed from the wrong target
                effective_schema = target_config.get("schema") or model.schema
            else:
                effective_schema = model.schema

            # model.database is set at parse time from the default target,
            # NOT from the model's config.target. For Databricks models that
            # specify config(target='dbx_dev'), model.database will still be
            # the default target's database (e.g. 'postgres' from pg_dev).
            # Use the actual target config's catalog/database instead.
            effective_database = (
                target_config.get("catalog")
                or target_config.get("database")
                or getattr(model, "database", None)
                or ""
            )

        # Build table name: catalog.schema.table or schema.table
        if effective_database and adapter_type in ("databricks", "spark"):
            return f"{effective_database}.{effective_schema}.{model.name}"
        else:
            return f"{effective_schema}.{model.name}"

    def _write_to_target(
        self,
        df: Any,
        model: Any,
        resolution: ResolvedExecution,
    ) -> LoadResult:
        """Write results to target database.

        Uses AdapterManager to get adapter for DDL operations with proper
        quoting per dialect. Data loading uses Spark JDBC.

        Priority:
        1. Bucket load (COPY INTO from cloud storage) - if bucket configured
        2. JDBC write (fallback)

        Args:
            df: Spark DataFrame with results
            model: ModelNode
            resolution: Resolved execution details

        Returns:
            LoadResult with success status and metadata
        """
        from dvt.federation.adapter_manager import AdapterManager

        # Get target connection config
        target_config = self._get_connection_config(resolution.target)
        if not target_config:
            return LoadResult(
                success=False,
                table_name=f"{model.schema}.{model.name}",
                error=f"No connection config for target '{resolution.target}'",
            )

        adapter_type = target_config.get("type", "")

        # Get adapter for DDL operations.
        # For session-limited adapters (Oracle, MSSQL), evict the cached
        # adapter to avoid thread-affinity issues: dbt's ConnectionManager
        # tracks connections per thread ID, and the cached adapter may hold
        # connections from a different thread (e.g., from extraction).
        if adapter_type in ("oracle", "sqlserver"):
            AdapterManager.evict(
                self._get_profile_name(),
                resolution.target,
            )
        adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )

        loader = get_loader(adapter_type, on_progress=self._log)

        # Determine write mode based on materialization
        mat = resolution.coerced_materialization or resolution.original_materialization

        # Check --full-refresh flag from CLI args
        full_refresh = getattr(
            getattr(self.config, "args", None), "FULL_REFRESH", False
        )

        if mat == "incremental" and not full_refresh:
            # Incremental: append new rows to existing table.
            # On first run the table may not exist — the loader handles this
            # by using Spark JDBC mode="append" which auto-creates if needed.
            mode = "append"
            truncate = False
        else:
            # Table/view materialization or incremental with --full-refresh:
            # DDL contract: TRUNCATE+INSERT (default) or DROP+CREATE (--full-refresh)
            mode = "overwrite"
            truncate = not full_refresh

        # Get JDBC load settings from computes.yml
        jdbc_config = self._get_jdbc_load_config()

        # Read incremental_strategy and unique_key from model config (Phase 4).
        # These drive merge/delete+insert load strategies in the loader.
        incremental_strategy = None
        unique_key = None
        if mat == "incremental" and not full_refresh:
            model_config = getattr(model, "config", None)
            if model_config:
                incremental_strategy = getattr(
                    model_config, "incremental_strategy", None
                )
                raw_unique_key = getattr(model_config, "unique_key", None)
                # Normalize unique_key to List[str] or None
                if isinstance(raw_unique_key, str):
                    unique_key = [raw_unique_key]
                elif isinstance(raw_unique_key, list):
                    unique_key = raw_unique_key
                else:
                    unique_key = None

        # Resolve target table name — handles --target schema/database override
        target_table_name = self._resolve_target_table_name(
            model, resolution, target_config
        )

        load_config = LoadConfig(
            table_name=target_table_name,
            mode=mode,
            truncate=truncate,
            full_refresh=full_refresh,
            connection_config=target_config,
            jdbc_config=jdbc_config,
            incremental_strategy=incremental_strategy,
            unique_key=unique_key,
        )

        return loader.load(df, load_config, adapter=adapter)

    def _cleanup_temp_views(self, spark: Any, view_prefix: str) -> None:
        """Remove temp views for this model.

        Args:
            spark: SparkSession
            view_prefix: Prefix used for this model's temp views
        """
        try:
            # Get all temp views
            catalog = spark.catalog
            tables = catalog.listTables()

            for table in tables:
                if table.name.startswith(view_prefix):
                    catalog.dropTempView(table.name)
        except Exception:
            pass  # Ignore cleanup errors

    # =========================================================================
    # Helper Methods
    # =========================================================================

    def _get_profile_name(self) -> str:
        """Get current profile name."""
        if hasattr(self.config, "profile_name"):
            return self.config.profile_name
        if hasattr(self.config, "profile"):
            return getattr(self.config.profile, "profile_name", "default")
        return "default"

    def _get_profiles_dir(self) -> Optional[str]:
        """Get profiles directory."""
        if hasattr(self.config, "profiles_dir"):
            return self.config.profiles_dir
        # Also check args which may have been passed when creating config
        if hasattr(self.config, "args") and hasattr(self.config.args, "profiles_dir"):
            return self.config.args.profiles_dir
        # Also check for profile property which may have profiles_dir
        if hasattr(self.config, "profile") and hasattr(
            self.config.profile, "profiles_dir"
        ):
            return self.config.profile.profiles_dir
        return None

    def _get_default_target(self) -> str:
        """Get default target from profile."""
        if hasattr(self.config, "target_name") and self.config.target_name:
            return self.config.target_name
        return "default"

    def _get_dialect_for_target(self, target: str) -> str:
        """Get SQL dialect for a target.

        Args:
            target: Target name

        Returns:
            Dialect name
        """
        config = self._get_connection_config(target)
        if config:
            adapter_type = config.get("type", "")
            return get_dialect_for_adapter(adapter_type)
        return "spark"

    def _get_connection_config(self, target: str) -> Optional[Dict[str, Any]]:
        """Get connection config for a target.

        Args:
            target: Target name

        Returns:
            Connection config dict or None
        """
        if self._profiles is None:
            profile_name = self._get_profile_name()
            profiles_dir = self._get_profiles_dir()
            try:
                self._profiles = load_profiles(profiles_dir)
            except Exception:
                self._profiles = {}

        profile_name = self._get_profile_name()
        profile = self._profiles.get(profile_name, {})
        outputs = profile.get("outputs", {})

        return outputs.get(target)

    def _get_bucket_config(self, bucket_name: str) -> Optional[Dict[str, Any]]:
        """Get bucket config.

        Args:
            bucket_name: Bucket name

        Returns:
            Bucket config dict or None
        """
        profile_name = self._get_profile_name()
        profiles_dir = self._get_profiles_dir()

        try:
            buckets = load_buckets_for_profile(profile_name, profiles_dir)
            if buckets:
                all_buckets = buckets.get("buckets", {})
                return all_buckets.get(bucket_name)
        except Exception:
            pass

        return None

    def _get_jdbc_load_config(self) -> Dict[str, Any]:
        """Get JDBC load settings from computes.yml.

        Returns:
            Dict with jdbc_load settings
        """
        profile_name = self._get_profile_name()
        profiles_dir = self._get_profiles_dir()

        try:
            computes = load_computes_for_profile(profile_name, profiles_dir)
            if computes:
                target_compute = computes.get("target", "")
                all_computes = computes.get("computes", {})
                compute_config = all_computes.get(target_compute, {})
                return compute_config.get("jdbc_load", {})
        except Exception:
            pass

        return {"num_partitions": 4, "batch_size": 10000}
