# Sling Integration

## Overview

Sling is DVT's data movement engine. It handles ALL data transfer between systems:
- Source extraction to default target
- Cross-target model materialization
- Seed loading (CSV → database)
- CDC (Change Data Capture)

DVT uses the `sling` Python wrapper (`pip install sling`) which calls the Sling Go binary.

## Sling Capabilities Summary

### What Sling Does
- Streams data between 30+ databases, file systems, and cloud buckets
- Supports custom SQL as source stream
- Handles incremental extraction via update_key watermarks
- Supports CDC via database transaction logs (MySQL binlog, Postgres WAL coming)
- Bulk loads using native database tools (COPY, bcp, sqlldr)
- Schema evolution (auto-adds new columns)
- Inline row-level transforms (trim, upper, hash, type casting)
- Configurable via CLI flags or YAML replication files
- Reads dbt profiles.yml natively for connection discovery
- Parallel stream execution

### What Sling Does NOT Do
- SQL-based transformations (no joins, aggregations, CTEs)
- DAG execution (no dependency ordering)
- Jinja templating
- Data testing

### Supported Connectors

**Databases:** PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, Snowflake, BigQuery,
Redshift, Databricks, DuckDB, ClickHouse, SQLite, StarRocks, Trino, Exasol,
MongoDB, Elasticsearch, Prometheus, Bigtable, Azure SQL/DWH/Table, Fabric,
Cloudflare D1, Proton

**File Systems:** Local, S3, GCS, Azure Blob, Backblaze B2, DigitalOcean Spaces,
MinIO, Cloudflare R2, Wasabi, SFTP

**File Formats:** CSV, Parquet, JSON, JSONLines, Avro, XML, Excel, SAS, GeoJSON

**Data Lakes:** Athena, Iceberg, DuckLake, S3 Tables

## How DVT Uses Sling

### 1. Sling Direct — Single Remote Source Extraction

When `dvt run` detects that a model references exactly ONE source whose `connection`
differs from the model's `target`, DVT uses **Sling Direct**: Sling streams data
directly from the source to the model's named table on the target.

No hidden staging tables. No `_dvt` schema. Result lands directly as the model's table.

```python
from sling import Replication, ReplicationStream

# DVT auto-generates this — the user never sees or configures it
replication = Replication(
    source=source_connection_url,       # from source's connection in sources.yml
    target=target_connection_url,       # model's target from profiles.yml
    streams={
        f"{source_schema}.{source_table}": ReplicationStream(
            object=f"{target_schema}.{model_name}",  # model's named table directly
            mode="full-refresh",                       # for table materialization
        ),
    },
)
replication.run()
```

For `incremental` models, Sling uses incremental mode with watermark filtering
and merges delta directly into the model's table (see watermark section below).

### 2. DuckDB Compute — Multiple Remote Source Extraction

When a model references 2+ remote sources, DVT uses **DuckDB Compute**:

1. Sling streams each remote source into DuckDB (in-memory)
2. Model SQL executes in DuckDB (user wrote DuckDB SQL)
3. Sling streams the result from DuckDB to the model's named table on the target

```python
import duckdb

# Step 1: Sling streams each remote source into DuckDB
duckdb_conn = duckdb.connect()  # ephemeral in-memory
for source_table in remote_source_tables:
    replication = Replication(
        source=source_connection_url,
        target="duckdb://",                 # in-memory DuckDB
        streams={
            f"{source_schema}.{source_table}": ReplicationStream(
                object=f"{source_name}__{table_name}",  # DuckDB table name
                mode="full-refresh",
            ),
        },
    )
    replication.run()

# Step 2: Execute model SQL in DuckDB
result = duckdb_conn.execute(compiled_model_sql)

# Step 3: Sling streams result from DuckDB to target
replication = Replication(
    source="duckdb://",
    target=target_connection_url,
    streams={
        "duckdb_result_table": ReplicationStream(
            object=f"{target_schema}.{model_name}",  # model's named table
            mode="full-refresh",
        ),
    },
)
replication.run()
duckdb_conn.close()
```

DuckDB Compute only supports `table` materialization. Incremental is not supported
for multi-source extraction (DVT112 error).

### 3. Seed Loading

When `dvt seed` is called:

```python
replication = Replication(
    source="file://.",                   # local filesystem
    target=target_connection_url,
    streams={
        f"file://{seed_path}": ReplicationStream(
            object=f"{schema}.{seed_name}",
            mode="full-refresh",         # or "truncate" for non-destructive
        ),
    },
)
replication.run()
```

**Advantages over dbt's native seed:**
- dbt uses Python agate library + batch INSERT statements
- Sling uses native bulk loading: COPY (Postgres), COPY INTO (Snowflake),
  bcp (SQL Server), etc.
- 10-100x faster for large seed files
- Better type inference
- Handles encoding issues (Latin-1, Windows-1252, etc.)

### 4. Cross-Target Loading

When a model has `config(target='other_db')`:

```python
# After model SQL executes on default target, result is in a temp table
# Sling streams from default target to model's target
replication = Replication(
    source=default_target_connection_url,
    target=model_target_connection_url,
    streams={
        "temp_schema.model_result_tmp": ReplicationStream(
            object=f"{schema}.{model_name}",
            mode="full-refresh",
        ),
    },
)
replication.run()
```

### 5. Bucket Materialization

When a model has `config(target='s3_bucket', format='delta')`:

```python
replication = Replication(
    source=default_target_connection_url,
    target=bucket_connection_url,
    streams={
        "temp_schema.model_result_tmp": ReplicationStream(
            object=f"s3://bucket/path/{model_name}/",
            target_options={
                "format": "delta",       # or parquet, csv, json
            },
        ),
    },
)
replication.run()
```

## Connection Mapper

DVT translates profiles.yml adapter configs to Sling connection URLs:

```python
ADAPTER_TO_SLING_URL = {
    "postgres":   "postgres://{user}:{password}@{host}:{port}/{dbname}?sslmode={sslmode}",
    "snowflake":  "snowflake://{user}:{password}@{account}/{database}?schema={schema}&warehouse={warehouse}&role={role}",
    "bigquery":   "bigquery://{project}?dataset={dataset}&location={location}",
    "redshift":   "redshift://{user}:{password}@{host}:{port}/{dbname}",
    "mysql":      "mysql://{user}:{password}@{host}:{port}/{database}",
    "sqlserver":  "sqlserver://{user}:{password}@{host}:{port}/{database}",
    "databricks": "databricks://{host}?token={token}&catalog={catalog}&schema={schema}",
    "oracle":     "oracle://{user}:{password}@{host}:{port}/{service}",
    "clickhouse": "clickhouse://{user}:{password}@{host}:{port}/{database}",
    "trino":      "trino://{user}@{host}:{port}/{catalog}?schema={schema}",
    "duckdb":     "duckdb://{path}",
    "sqlite":     "sqlite://{path}",
    # Bucket types
    "s3":         "s3://{bucket}?region={region}&access_key_id={access_key_id}&secret_access_key={secret_access_key}",
    "gcs":        "gs://{bucket}?project={project}",
    "azure":      "azure://{account}/{container}?access_key={access_key}",
}
```

This mapping lives in `core/dvt/extraction/connection_mapper.py`. It handles:
- URL construction from profile fields
- Environment variable resolution (Jinja rendering happens before mapping)
- Special auth methods (key-pair, OAuth, service accounts)

## Sling Modes Mapped from dbt Materializations

DVT maps dbt model materializations to Sling modes for extraction models:

### table → Sling full-refresh (direct to model table)

**Sling Direct (single source):** Sling streams source data directly to the model's
named table on the target using `full-refresh` mode.

**DuckDB Compute (multi-source):** Sling streams each source into DuckDB, model SQL
runs in DuckDB, then Sling streams the result to the model's table on the target.

### incremental → Sling extraction with watermark filtering (single source only)

Only supported for **Sling Direct** (single remote source). DVT:
- Pre-resolves the watermark from the model's target table in dialect-specific format
- Filters the Sling extraction query to only pull delta rows
- Sling merges delta directly into the model's named table on the target
- `unique_key` maps to Sling's `primary_key` for the merge

Multi-source incremental is not supported (DVT112 error).

### --full-refresh

- `is_incremental()` returns false → no watermark filter
- Sling re-extracts full source table → model table on target
- Matches stock dbt `--full-refresh` behavior exactly

## Dialect-Specific Watermark Formatting

When DVT pre-resolves the watermark for cross-engine incremental models, it must
format the value as a valid SQL literal for the **source** database's dialect.

DVT maintains a `watermark_formatter.py` with dialect-specific templates:

| Source Engine | Timestamp Format | Date Format |
|---|---|---|
| PostgreSQL | `'{value}'::TIMESTAMP` | `'{value}'::DATE` |
| MySQL/MariaDB | `'{value}'` | `'{value}'` |
| SQL Server | `CONVERT(DATETIME2, '{value}', 121)` | `CONVERT(DATE, '{value}', 23)` |
| Oracle | `TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS.FF6')` | `TO_DATE('{value}', 'YYYY-MM-DD')` |
| Snowflake | `TO_TIMESTAMP('{value}')` | `TO_DATE('{value}')` |
| BigQuery | `TIMESTAMP '{value}'` | `DATE '{value}'` |
| Redshift | `'{value}'::TIMESTAMP` | `'{value}'::DATE` |
| Databricks | `TIMESTAMP '{value}'` | `DATE '{value}'` |
| ClickHouse | `toDateTime64('{value}', 6)` | `toDate('{value}')` |
| Trino | `TIMESTAMP '{value}'` | `DATE '{value}'` |

Integer/numeric watermarks: plain literals (`12345`).
String watermarks: `'value'` (SQL Server: `N'value'`).

## Sling Type Mapping

Sling uses a two-step type mapping: source native → Sling general → target native.

**General types:** `bigint`, `integer`, `smallint`, `float`, `decimal`, `string`,
`text`, `bool`, `date`, `datetime`, `timestamp`, `timestampz`, `time`, `json`,
`binary`, `uuid`.

Standard analytics types (integers, floats, strings, booleans, dates, timestamps,
decimals) map cleanly across all engines. No user intervention needed.

Exotic types (`GEOGRAPHY`, `HSTORE`, `TSVECTOR`, `INTERVAL`, nested ARRAY/STRUCT)
are mapped to `text` or `json` with a DVT002 warning.

DVT manages column type mapping internally via Sling's general type system.
No user configuration is needed — standard analytics types map cleanly across engines.

## Sling Transforms and Hooks

Sling supports 50+ built-in transform functions and a hook system (pre/post SQL,
checks, webhooks). These are capabilities of Sling that DVT may leverage internally
for staging extractions, but they are NOT exposed as user-facing model config.
Users perform all data transformations in standard dbt SQL models on the target.

## Error Handling

When a Sling extraction fails:
1. The model that triggered the extraction is marked as FAILED in the DVT DAG
2. All downstream models that depend on this model are SKIPPED
3. The error message from Sling is surfaced in DVT's run results
4. `dvt retry` will re-attempt failed models and their downstream dependencies

For incremental extractions, failure is safe — the watermark was not updated,
so the next run will re-attempt the same range.

For CDC, Sling's own retry logic handles transient failures (configurable
`retry_attempts` and `retry_delay`).
