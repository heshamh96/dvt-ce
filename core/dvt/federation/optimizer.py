"""
DVT Federation Optimizer — reduce data movement for extraction models.

Analyzes the model's DuckDB SQL to determine:
1. Column pruning: which columns are actually used from each source
2. Predicate pushdown: which WHERE clauses can be pushed to source extraction
3. LIMIT pushdown: if the model has a LIMIT, propagate to extraction

This generates optimized extraction queries per source instead of SELECT *.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

# Maps dbt adapter type → SQLGlot dialect name
ADAPTER_TO_SQLGLOT_DIALECT: Dict[str, str] = {
    "postgres": "postgres",
    "redshift": "redshift",
    "mysql": "mysql",
    "mariadb": "mysql",
    "mysql5": "mysql",
    "sqlserver": "tsql",
    "oracle": "oracle",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "databricks": "databricks",
    "spark": "spark",
    "clickhouse": "clickhouse",
    "trino": "trino",
    "duckdb": "duckdb",
    "sqlite": "sqlite",
}


@dataclass
class OptimizedExtraction:
    """Optimized extraction query for a single source table."""

    cache_table: str  # DuckDB cache table name (e.g., mysql_source__test_seed)
    columns: List[str]  # Columns to extract (empty = SELECT *)
    predicates: List[str]  # WHERE predicates to push down (source-dialect formatted)
    limit: Optional[int]  # LIMIT to push down (None = no limit)

    def build_query(
        self,
        source_schema: str,
        source_table: str,
        source_dialect: str = "duckdb",
    ) -> str:
        """Build the optimized extraction SQL for the source.

        Transpiles predicates from DuckDB SQL to the source's dialect
        via SQLGlot, so pushdown queries are valid on the source engine.

        Args:
            source_schema: Schema on the source database.
            source_table: Table name on the source database.
            source_dialect: SQLGlot dialect name for the source engine.
        """
        # Column selection
        if self.columns:
            col_list = ", ".join(self.columns)
        else:
            col_list = "*"

        # Table reference
        if source_schema:
            table_ref = f"{source_schema}.{source_table}"
        else:
            table_ref = source_table

        sql = f"SELECT {col_list} FROM {table_ref}"

        # WHERE predicates — transpile from DuckDB to source dialect
        if self.predicates:
            transpiled_preds = []
            for pred in self.predicates:
                try:
                    transpiled = sqlglot.transpile(
                        pred, read="duckdb", write=source_dialect, pretty=False
                    )[0]
                    transpiled_preds.append(transpiled)
                except Exception:
                    # If transpilation fails, use the predicate as-is
                    transpiled_preds.append(pred)
            sql += " WHERE " + " AND ".join(transpiled_preds)

        # LIMIT
        if self.limit is not None:
            sql += f" LIMIT {self.limit}"

        return sql


def optimize_extractions(
    compiled_sql: str,
    source_table_map: Dict[str, str],
) -> Dict[str, OptimizedExtraction]:
    """Analyze model SQL and generate optimized extraction queries per source.

    Args:
        compiled_sql: The model's compiled DuckDB SQL (after source ref rewriting
                      to cache table names).
        source_table_map: Map of cache_table_name → original source info.
                          E.g., {"mysql_source__test_seed": "mysql_source__test_seed"}

    Returns:
        Dict mapping cache_table_name → OptimizedExtraction.
        If optimization fails, returns empty dict (caller falls back to SELECT *).
    """
    try:
        parsed = sqlglot.parse_one(compiled_sql, dialect="duckdb")
    except Exception as e:
        logger.debug(f"Federation optimizer: SQL parse failed: {e}")
        return {}

    if not isinstance(parsed, exp.Select):
        return {}

    # Build table alias → cache table name mapping
    alias_to_cache = _build_alias_map(parsed, source_table_map)
    if not alias_to_cache:
        return {}

    # Extract columns per table
    columns_per_table = _extract_columns(parsed, alias_to_cache)

    # Extract pushable predicates per table
    predicates_per_table = _extract_predicates(parsed, alias_to_cache)

    # Extract LIMIT
    limit_value = _extract_limit(parsed)

    # Build optimized extractions
    result: Dict[str, OptimizedExtraction] = {}
    for cache_table in source_table_map:
        columns = sorted(columns_per_table.get(cache_table, set()))
        predicates = predicates_per_table.get(cache_table, [])

        # Only apply LIMIT if the query is a simple single-table select
        table_limit = limit_value if len(source_table_map) == 1 else None

        result[cache_table] = OptimizedExtraction(
            cache_table=cache_table,
            columns=columns,
            predicates=predicates,
            limit=table_limit,
        )

    return result


def _build_alias_map(
    parsed: exp.Select,
    source_table_map: Dict[str, str],
) -> Dict[str, str]:
    """Build a mapping of table alias → cache table name.

    Handles: FROM table_name alias, FROM table_name AS alias, FROM table_name
    """
    alias_map: Dict[str, str] = {}

    for table in parsed.find_all(exp.Table):
        table_name = table.name
        alias = table.alias or table_name

        if table_name in source_table_map:
            alias_map[alias] = table_name
        # Also check without quotes
        clean_name = table_name.strip('"').strip("'").strip("`")
        if clean_name in source_table_map:
            alias_map[alias] = clean_name

    return alias_map


def _extract_columns(
    parsed: exp.Select,
    alias_to_cache: Dict[str, str],
) -> Dict[str, Set[str]]:
    """Extract which columns are used from each source table.

    Returns: {cache_table_name: {col1, col2, ...}}
    """
    columns: Dict[str, Set[str]] = {}

    for col in parsed.find_all(exp.Column):
        table_alias = col.table
        col_name = col.name

        if not table_alias:
            # Unqualified column — if only one source table, attribute to it
            if len(alias_to_cache) == 1:
                cache_table = list(alias_to_cache.values())[0]
                if cache_table not in columns:
                    columns[cache_table] = set()
                columns[cache_table].add(col_name)
            continue

        cache_table = alias_to_cache.get(table_alias)
        if cache_table:
            if cache_table not in columns:
                columns[cache_table] = set()
            columns[cache_table].add(col_name)

    # For tables with columns, also include JOIN key columns
    for join_expr in parsed.find_all(exp.Join):
        on_clause = join_expr.args.get("on")
        if on_clause:
            for col in on_clause.find_all(exp.Column):
                table_alias = col.table
                col_name = col.name
                if table_alias:
                    cache_table = alias_to_cache.get(table_alias)
                    if cache_table and cache_table in columns:
                        columns[cache_table].add(col_name)

    return columns


def _extract_predicates(
    parsed: exp.Select,
    alias_to_cache: Dict[str, str],
) -> Dict[str, List[str]]:
    """Extract WHERE predicates that can be pushed to a single source.

    A predicate is pushable if ALL its column references belong to one table.

    Returns: {cache_table_name: ["col > 20", "status = 'active'"]}
    """
    predicates: Dict[str, List[str]] = {}

    where = parsed.find(exp.Where)
    if not where:
        return predicates

    # Split AND conditions
    conditions = _split_and(where.this)

    for condition in conditions:
        # Find all table references in this condition
        tables_in_condition = set()
        has_unqualified = False
        for col in condition.find_all(exp.Column):
            if col.table:
                cache_table = alias_to_cache.get(col.table)
                if cache_table:
                    tables_in_condition.add(cache_table)
            else:
                has_unqualified = True

        # Unqualified columns in single-table queries → attribute to that table
        if (
            has_unqualified
            and len(alias_to_cache) == 1
            and len(tables_in_condition) == 0
        ):
            tables_in_condition.add(list(alias_to_cache.values())[0])

        # Only push if ALL columns belong to exactly ONE source table
        if len(tables_in_condition) == 1:
            cache_table = tables_in_condition.pop()
            # Generate the predicate SQL without the table alias
            pred_sql = _strip_table_alias(condition, alias_to_cache)
            if cache_table not in predicates:
                predicates[cache_table] = []
            predicates[cache_table].append(pred_sql)

    return predicates


def _extract_limit(parsed: exp.Select) -> Optional[int]:
    """Extract LIMIT value from the query."""
    limit = parsed.find(exp.Limit)
    if limit and limit.expression:
        try:
            return int(limit.expression.this)
        except (ValueError, AttributeError):
            pass
    return None


def _split_and(expr: exp.Expression) -> List[exp.Expression]:
    """Split an AND expression into individual conditions."""
    if isinstance(expr, exp.And):
        return _split_and(expr.left) + _split_and(expr.right)
    return [expr]


def _strip_table_alias(
    condition: exp.Expression,
    alias_to_cache: Dict[str, str],
) -> str:
    """Convert a condition expression to SQL, stripping table aliases.

    E.g., "o.amount > 20" → "amount > 20"
    """
    # Clone to avoid modifying the original AST
    cloned = condition.copy()
    for col in cloned.find_all(exp.Column):
        if col.table:
            # Remove the table qualifier
            col.set("table", None)
    return cloned.sql(dialect="duckdb")
