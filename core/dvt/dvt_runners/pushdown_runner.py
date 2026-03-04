"""DVT NonDefaultPushdownRunner — executes models via pushdown on non-default target adapters.

When a model targets a non-default adapter (e.g., databricks when the default profile
is postgres), the standard ModelRunner cannot be used because it's bound to the default
adapter via the global FACTORY singleton.

This runner uses AdapterManager to get the correct target adapter and executes the
compiled SQL directly via CREATE TABLE AS / CREATE VIEW AS.
"""

from __future__ import annotations

import threading
import time
from typing import Any

from dvt.adapters.base import BaseAdapter
from dvt.artifacts.schemas.results import RunStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.config import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.dvt_runners.runner_mixin import DvtRunnerMixin
from dvt.exceptions import DbtRuntimeError
from dvt.federation.resolver import ResolvedExecution
from dvt.task.compile import CompileRunner


class NonDefaultPushdownRunner(DvtRunnerMixin, CompileRunner):
    """Runner for pushdown execution on non-default target adapters.

    The compiled SQL is already in the target dialect (Jinja resolved during
    the compile phase). We wrap it in the appropriate DDL and execute on
    the target adapter.
    """

    def __init__(
        self,
        config: RuntimeConfig,
        adapter: BaseAdapter,
        node: ModelNode,
        node_index: int,
        num_nodes: int,
        resolution: ResolvedExecution,
        manifest: Manifest,
    ):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self._init_dvt_runner(resolution, manifest)

    def describe_node(self) -> str:
        return (
            f"{self.node.language} {self.node.get_materialization()} model "
            f"{self.get_node_representation()} "
            f"[pushdown -> {self.resolution.target}]"
        )

    def compile(self, manifest: Manifest):
        """Compile with target-aware adapter for correct dialect.

        IMPORTANT: Fix node schema/database BEFORE compilation so that
        Jinja rendering with the target adapter creates Relations with
        the correct schema. Without this, MySQL/MariaDB adapters reject
        Relations where database != schema (e.g., database='devdb',
        schema='public').
        """
        from dvt.federation.adapter_manager import AdapterManager

        target_adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=self.resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )
        self._fix_node_schema_for_target(target_adapter)
        return self._compile_with_target_adapter(manifest)

    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Execute model via pushdown on a non-default target adapter."""
        from dvt.federation.adapter_manager import AdapterManager, get_quoted_table_name

        start_time = time.time()

        compiled_sql = model.compiled_code
        if not compiled_sql:
            raise DbtRuntimeError(f"Model {model.unique_id} has no compiled SQL")

        target_adapter = AdapterManager.get_adapter(
            profile_name=self._get_profile_name(),
            target_name=self.resolution.target,
            profiles_dir=self._get_profiles_dir(),
        )

        # Fix node schema/database to match the target adapter credentials.
        # At parse time, model.schema is set from the default profile target
        # (e.g., 'public' for Postgres), which is wrong for non-default
        # targets (e.g., MySQL uses 'devdb', Oracle uses 'SYSTEM', etc.).
        self._fix_node_schema_for_target(target_adapter)

        # Strip cross-database references from compiled SQL.
        # Source references are compiled with the default target's database
        # (e.g., "devdb"."public"."table"), but pushdown runs on a different
        # target where this database prefix creates invalid cross-db refs.
        # Since pushdown models only reference tables within the same
        # database, we strip the database qualifier.
        compiled_sql = self._strip_database_from_sql(compiled_sql, target_adapter)

        materialization = model.config.materialized or "table"
        full_refresh = getattr(self.config.args, "FULL_REFRESH", False)

        table_name = f"{model.schema}.{model.name}"
        quoted_table = get_quoted_table_name(target_adapter, table_name)

        # Retry logic for stale connections (MSSQL/Oracle adapters may have
        # timed out between federation operations and pushdown execution).
        max_attempts = 2
        last_error = None

        for attempt in range(max_attempts):
            try:
                conn_name = f"dvt_pushdown_{model.name}_{attempt}"
                with target_adapter.connection_named(conn_name):
                    if materialization == "view":
                        self._execute_view(target_adapter, quoted_table, compiled_sql)
                        message = f"CREATE VIEW {quoted_table}"
                    elif materialization == "incremental" and not full_refresh:
                        self._execute_incremental(
                            target_adapter, quoted_table, compiled_sql, model
                        )
                        message = f"INSERT INTO {quoted_table}"
                    else:
                        self._execute_table(
                            target_adapter,
                            quoted_table,
                            compiled_sql,
                            full_refresh,
                        )
                        message = f"CREATE TABLE {quoted_table}"

                elapsed = time.time() - start_time
                return RunResult(
                    node=model,
                    status=RunStatus.Success,
                    timing=[],
                    thread_id=threading.current_thread().name,
                    execution_time=elapsed,
                    message=message,
                    adapter_response={
                        "pushdown": True,
                        "target": self.resolution.target,
                    },
                    failures=0,
                    batch_results=None,
                )

            except Exception as e:
                last_error = e
                err_lower = str(e).lower()
                is_connection_error = any(
                    phrase in err_lower
                    for phrase in [
                        "closed connection",
                        "not connected",
                        "connection reset",
                        "broken pipe",
                        "connection refused",
                        "connection timed out",
                        "communication link failure",
                    ]
                )
                if is_connection_error and attempt < max_attempts - 1:
                    # Evict the stale adapter from cache and get a fresh one.
                    # The cached adapter's connection pool holds dead connections
                    # that can't be recovered by cleanup alone.
                    try:
                        target_adapter.connections.cleanup_all()
                    except Exception:
                        pass
                    try:
                        AdapterManager.evict(
                            self._get_profile_name(),
                            self.resolution.target,
                        )
                    except Exception:
                        pass
                    # Get a fresh adapter for the retry
                    target_adapter = AdapterManager.get_adapter(
                        profile_name=self._get_profile_name(),
                        target_name=self.resolution.target,
                        profiles_dir=self._get_profiles_dir(),
                    )
                    # Re-derive quoted table name with fresh adapter
                    quoted_table = get_quoted_table_name(target_adapter, table_name)
                    continue
                break

        elapsed = time.time() - start_time
        return RunResult(
            node=model,
            status=RunStatus.Error,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=elapsed,
            message=f"Pushdown failed: {last_error}",
            adapter_response={
                "pushdown": True,
                "target": self.resolution.target,
                "error": str(last_error),
            },
            failures=1,
            batch_results=None,
        )

    # -- SQL rewrite helpers --

    @staticmethod
    def _strip_database_from_sql(compiled_sql: str, target_adapter: Any) -> str:
        """Remove database/catalog qualifiers from table references in SQL.

        Pushdown models execute on a single database engine, so all table
        references are within the same database.  But source references
        compiled by the default adapter include a database prefix (e.g.,
        "devdb"."public"."table") that creates invalid cross-database refs
        on engines like PostgreSQL.

        Uses SQLGlot to parse the SQL and strip the catalog from all table
        references, leaving schema.table or just table.
        """
        try:
            import sqlglot
            from sqlglot import exp
            from dvt.federation.dialect_fallbacks import get_dialect_for_adapter

            adapter_type = target_adapter.type()
            dialect = get_dialect_for_adapter(adapter_type)
            # Parse with the target dialect — the SQL has already been
            # transpiled (backticks for Databricks, etc.). Unlike
            # _translate_to_spark() where input SQL has double-quoted
            # identifiers, here the SQL is already in the target dialect.
            parsed = sqlglot.parse_one(compiled_sql, read=dialect)

            for table in parsed.find_all(exp.Table):
                if table.catalog:
                    table.set("catalog", None)

            return parsed.sql(dialect=dialect)
        except Exception:
            # Fallback: return SQL unchanged if parsing fails
            return compiled_sql

    # -- DDL helpers --

    def _execute_table(
        self,
        adapter: Any,
        quoted_table: str,
        compiled_sql: str,
        full_refresh: bool,
    ) -> None:
        """Execute table materialization on target adapter."""
        adapter_type = adapter.type()

        # Drop existing table/view using dialect-appropriate syntax
        self._drop_table_or_view(adapter, adapter_type, quoted_table)

        # Create table as select (dialect-specific syntax)
        if adapter_type == "sqlserver":
            # MSSQL: SELECT ... INTO table (not CREATE TABLE ... AS SELECT)
            # Inject INTO clause after SELECT keyword
            ctas_sql = self._build_select_into(compiled_sql, quoted_table)
            adapter.execute(ctas_sql, auto_begin=True)
        else:
            adapter.execute(
                f"CREATE TABLE {quoted_table} AS {compiled_sql}",
                auto_begin=True,
            )
        try:
            adapter.connections.get_thread_connection().handle.commit()
        except Exception:
            pass

    def _drop_table_or_view(
        self,
        adapter: Any,
        adapter_type: str,
        quoted_table: str,
    ) -> None:
        """Drop a table or view using dialect-appropriate DDL.

        Oracle doesn't support IF EXISTS or CASCADE on DROP statements.
        MSSQL uses different syntax for IF EXISTS.
        """
        if adapter_type == "oracle":
            # Oracle: use PL/SQL exception handling for conditional drop
            for obj_type in ("TABLE", "VIEW"):
                try:
                    adapter.execute(
                        f"BEGIN EXECUTE IMMEDIATE 'DROP {obj_type} {quoted_table}'; "
                        f"EXCEPTION WHEN OTHERS THEN NULL; END;",
                        auto_begin=True,
                    )
                    try:
                        adapter.connections.get_thread_connection().handle.commit()
                    except Exception:
                        pass
                except Exception:
                    pass
        elif adapter_type == "sqlserver":
            # MSSQL: use IF OBJECT_ID syntax
            for obj_type, obj_code in [("TABLE", "U"), ("VIEW", "V")]:
                try:
                    adapter.execute(
                        f"IF OBJECT_ID('{quoted_table}', '{obj_code}') IS NOT NULL "
                        f"DROP {obj_type} {quoted_table}",
                        auto_begin=True,
                    )
                    try:
                        adapter.connections.get_thread_connection().handle.commit()
                    except Exception:
                        pass
                except Exception:
                    pass
        else:
            # Standard SQL (Postgres, MySQL, MariaDB, etc.)
            for obj_type in ("TABLE", "VIEW"):
                try:
                    adapter.execute(f"DROP {obj_type} IF EXISTS {quoted_table} CASCADE")
                except Exception:
                    try:
                        adapter.execute(f"DROP {obj_type} IF EXISTS {quoted_table}")
                    except Exception:
                        pass

    def _execute_view(
        self,
        adapter: Any,
        quoted_table: str,
        compiled_sql: str,
    ) -> None:
        """Execute view materialization on target adapter."""
        adapter.execute(f"CREATE OR REPLACE VIEW {quoted_table} AS {compiled_sql}")

    def _execute_incremental(
        self,
        adapter: Any,
        quoted_table: str,
        compiled_sql: str,
        model: ModelNode,
    ) -> None:
        """Execute incremental materialization on target adapter."""
        sql_stripped = compiled_sql.strip().upper()
        if sql_stripped.startswith("SELECT") or sql_stripped.startswith("WITH"):
            adapter.execute(f"INSERT INTO {quoted_table} {compiled_sql}")
        else:
            adapter.execute(compiled_sql)

    @staticmethod
    def _build_select_into(compiled_sql: str, quoted_table: str) -> str:
        """Build MSSQL SELECT ... INTO statement from a SELECT query.

        MSSQL doesn't support CREATE TABLE ... AS SELECT. Instead, it uses
        SELECT ... INTO target_table FROM ....

        The safest approach: wrap the compiled SQL as a subquery and use
        SELECT * INTO from it. This avoids complex regex parsing of SQL
        with comments, CTEs, etc.

        Args:
            compiled_sql: The compiled SELECT or WITH query
            quoted_table: Target table name (quoted)

        Returns:
            MSSQL-compatible SELECT ... INTO query
        """
        return f"SELECT * INTO {quoted_table} FROM ({compiled_sql}) AS _dvt_src"
