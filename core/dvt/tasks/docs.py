"""
DVT Docs Generate Task — extends dbt's GenerateTask with cross-engine catalog.

For sources with connection: (remote engines), uses the adapter's database
driver to query information_schema for table/column metadata.
For local sources (no connection:), uses the default adapter (stock dbt).

The result is a unified catalog across all engines.
"""

import csv
import json
import logging
import os
import tempfile
from typing import Any, Dict, List, Optional

from dvt.config.source_connections import load_source_connections
from dvt.sync.profiles_reader import default_profiles_dir, read_profiles_yml

logger = logging.getLogger(__name__)


# Engine-specific SQL for column metadata
METADATA_SQL = {
    "postgres": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
    "redshift": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
    "mysql": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
    "mariadb": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
    "sqlserver": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
    "snowflake": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
    "bigquery": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM `{schema}`.INFORMATION_SCHEMA.COLUMNS "
        "WHERE table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
    "oracle": (
        "SELECT column_name, data_type, column_id as ordinal_position "
        "FROM all_tab_columns "
        "WHERE owner = '{schema}' AND table_name = UPPER('{table}') "
        "ORDER BY column_id"
    ),
    "databricks": (
        "SELECT column_name, data_type, ordinal_position "
        "FROM information_schema.columns "
        "WHERE table_schema = '{schema}' AND table_name = '{table}' "
        "ORDER BY ordinal_position"
    ),
}


def enrich_catalog_with_remote_sources(
    catalog_path: str,
    manifest: Any,
    project_dir: str,
    profiles_dir: str,
) -> None:
    """Enrich catalog.json with metadata from remote source engines."""
    source_connections = load_source_connections(project_dir)
    if not source_connections:
        return

    try:
        profiles = read_profiles_yml(profiles_dir)
    except Exception:
        return

    if not os.path.isfile(catalog_path):
        return
    with open(catalog_path, "r") as f:
        catalog = json.load(f)

    # Group remote sources by connection
    conn_sources: Dict[str, List[Any]] = {}
    for uid, source_node in manifest.sources.items():
        src_name = getattr(source_node, "source_name", "")
        connection = source_connections.get(src_name)
        if not connection:
            continue
        if connection not in conn_sources:
            conn_sources[connection] = []
        conn_sources[connection].append(source_node)

    if not conn_sources:
        return

    updated = False

    for connection_name, source_nodes in conn_sources.items():
        conn_config = _get_output_config(connection_name, profiles)
        if not conn_config:
            continue

        adapter_type = conn_config.get("type", "")

        for source_node in source_nodes:
            uid = getattr(source_node, "unique_id", "")
            schema = getattr(source_node, "schema", "")
            table = getattr(source_node, "identifier", "") or getattr(
                source_node, "name", ""
            )
            database = getattr(source_node, "database", "")

            columns = _query_columns(adapter_type, conn_config, schema, table)

            if columns:
                catalog.setdefault("sources", {})[uid] = {
                    "metadata": {
                        "type": "BASE TABLE",
                        "schema": schema,
                        "name": table,
                        "database": database,
                        "comment": None,
                        "owner": connection_name,
                    },
                    "columns": columns,
                    "stats": {},
                    "unique_id": uid,
                }
                updated = True
                logger.info(
                    f"dvt docs: {getattr(source_node, 'source_name', '')}.{table} → "
                    f"{len(columns)} columns from {adapter_type} ({connection_name})"
                )

    if updated:
        with open(catalog_path, "w") as f:
            json.dump(catalog, f, indent=2)


def _query_columns(
    adapter_type: str,
    conn_config: Dict[str, Any],
    schema: str,
    table: str,
) -> Dict[str, Dict[str, Any]]:
    """Query column metadata from a remote engine using its native driver."""
    conn = _get_connection(adapter_type, conn_config)
    if not conn:
        return {}

    sql_template = METADATA_SQL.get(adapter_type)
    if not sql_template:
        return {}

    sql = sql_template.format(schema=schema, table=table)

    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        columns = {}
        for row in rows:
            col_name = str(row[0]).lower()
            col_type = str(row[1])
            col_index = int(row[2]) if row[2] else 0
            columns[col_name] = {
                "type": col_type,
                "index": col_index,
                "name": col_name,
                "comment": None,
            }
        return columns
    except Exception as e:
        logger.debug(f"dvt docs: metadata query failed ({adapter_type}): {e}")
        try:
            conn.close()
        except Exception:
            pass
        return {}


def _get_connection(adapter_type: str, config: Dict[str, Any]):
    """Get a database connection using the adapter's native Python driver."""
    try:
        if adapter_type == "postgres" or adapter_type == "redshift":
            return _connect_postgres(config)
        elif adapter_type in ("mysql", "mariadb"):
            return _connect_mysql(config)
        elif adapter_type == "sqlserver":
            return _connect_sqlserver(config)
        elif adapter_type == "oracle":
            return _connect_oracle(config)
        elif adapter_type == "snowflake":
            return _connect_snowflake(config)
        elif adapter_type == "databricks":
            return _connect_databricks(config)
    except Exception as e:
        logger.debug(f"dvt docs: could not connect to {adapter_type}: {e}")
    return None


def _connect_postgres(config):
    import psycopg2

    return psycopg2.connect(
        host=config.get("host", "localhost"),
        port=config.get("port", 5432),
        user=config.get("user", ""),
        password=config.get("password", config.get("pass", "")),
        dbname=config.get("dbname", config.get("database", "")),
    )


def _connect_mysql(config):
    import mysql.connector

    return mysql.connector.connect(
        host=config.get("host", config.get("server", "localhost")),
        port=config.get("port", 3306),
        user=config.get("user", config.get("username", "")),
        password=config.get("password", config.get("pass", "")),
        database=config.get("database", config.get("schema", "")),
    )


def _connect_sqlserver(config):
    import pyodbc

    host = config.get("host", config.get("server", ""))
    port = config.get("port", 1433)
    user = config.get("user", "")
    password = config.get("password", config.get("pass", ""))
    database = config.get("database", "")
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={host},{port};"
        f"DATABASE={database};"
        f"UID={user};PWD={password};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def _connect_oracle(config):
    import oracledb

    host = config.get("host", "")
    port = config.get("port", 1521)
    service = config.get("service", config.get("database", ""))
    user = config.get("user", "")
    password = config.get("password", config.get("pass", ""))
    dsn = f"{host}:{port}/{service}"
    return oracledb.connect(user=user, password=password, dsn=dsn)


def _connect_snowflake(config):
    import snowflake.connector

    return snowflake.connector.connect(
        account=config.get("account", ""),
        user=config.get("user", ""),
        password=config.get("password", config.get("pass", "")),
        database=config.get("database", ""),
        schema=config.get("schema", ""),
        warehouse=config.get("warehouse", ""),
    )


def _connect_databricks(config):
    from databricks import sql

    return sql.connect(
        server_hostname=config.get("host", ""),
        http_path=config.get("http_path", ""),
        access_token=config.get("token", ""),
        catalog=config.get("catalog", ""),
        schema=config.get("schema", ""),
    )


def _get_output_config(
    output_name: str, profiles: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    for profile_name, profile_data in profiles.items():
        if not isinstance(profile_data, dict):
            continue
        outputs = profile_data.get("outputs", {})
        if output_name in outputs:
            return outputs[output_name]
    return None
