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

### 1. Source Extraction

When `dvt run` encounters an extraction node:

```python
from sling import Replication, ReplicationStream

# DVT generates this from sources.yml + profiles.yml
replication = Replication(
    source=source_connection_url,       # mapped from profiles.yml
    target=target_connection_url,       # default target from profiles.yml
    streams={
        "public.customers": ReplicationStream(
            object="dvt_raw.crm__customers",
            mode="incremental",
            primary_key=["id"],
            update_key="updated_at",
        ),
    },
)
replication.run()
```

**With predicate pushdown (federation optimizer):**
```python
# Instead of extracting all columns and all rows,
# DVT analyzes downstream models to determine what's needed
streams={
    "public.customers.custom": ReplicationStream(
        sql="SELECT id, name, email, updated_at FROM public.customers WHERE region = 'US'",
        object="dvt_raw.crm__customers",
        mode="incremental",
        primary_key=["id"],
        update_key="updated_at",
    ),
}
```

### 2. Seed Loading

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

### 3. Cross-Target Loading

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

### 4. Bucket Materialization

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

## Sling Extraction Modes in DVT Context

### full-refresh

```yaml
# sources.yml
- name: ref_countries
  sling:
    mode: full-refresh
```

- Sling drops and recreates the extraction table every run
- Use for small reference tables that change rarely
- No state tracking needed

### incremental (with primary_key + update_key)

```yaml
# sources.yml
- name: orders
  sling:
    mode: incremental
    primary_key: [id]
    update_key: modified_at
```

- First run: full table load
- Subsequent runs: Sling queries `MAX(update_key)` from the extraction table
  and only fetches rows where `update_key > watermark`
- Fetched rows are upserted (matched on primary_key)
- State is implicit: the watermark lives in the extraction table itself

### incremental (with primary_key only)

```yaml
# sources.yml
- name: products
  sling:
    mode: incremental
    primary_key: [product_id]
```

- Every run: full data upsert (Sling reads entire source, merges on primary_key)
- More expensive than update_key-based incremental but catches all changes
- Good for tables without a reliable updated_at column

### change-capture (CDC)

```yaml
# sources.yml
- name: events
  sling:
    mode: change-capture
    primary_key: [event_id]
    change_capture_options:
      run_max_events: 50000
      soft_delete: true
```

- First run: automatic initial snapshot (chunked for large tables)
- Subsequent runs: reads database transaction log from last position
- Captures inserts, updates, AND deletes
- Adds metadata columns: `_sling_synced_at`, `_sling_synced_op`, `_sling_cdc_seq`
- Requires `SLING_STATE` env var for position tracking
- Currently supports: MySQL/MariaDB (binlog). Postgres (WAL) coming soon.
- NOTE: CDC requires Sling Pro Max license

### snapshot

```yaml
# sources.yml
- name: daily_metrics
  sling:
    mode: snapshot
```

- Every run: appends full dataset with `_sling_loaded_at` timestamp
- Keeps full history of how the source looked at each extraction point
- Good for Type 2 SCD patterns or audit trails

### backfill

```yaml
# sources.yml
- name: historical_orders
  sling:
    mode: backfill
    primary_key: [id]
    update_key: order_date
    source_options:
      range: "2020-01-01,2024-01-01"
```

- One-time or manual backfill of a specific range
- Useful for historical data loading or re-processing a time window

## Sling Transforms (Inline Row-Level)

Sling supports 50+ built-in transform functions applied during extraction.
These are NOT SQL transforms — they're applied row-by-row in Go during streaming.

```yaml
# sources.yml
- name: customers
  sling:
    transforms:
      email: ["lower", "trim_space"]
      phone: ["replace_non_printable"]
      name: ["trim_space", "replace_accents"]
```

Available functions: `upper`, `lower`, `trim_space`, `replace_accents`,
`replace_non_printable`, `hash` (md5/sha256), `cast`, `coalesce`,
`date_parse`, `date_format`, `int_parse`, `float_parse`, `uuid`, etc.

These are useful for data cleansing during extraction, before the data
reaches the target for SQL-based transformation.

## Sling Hooks in DVT Context

Sling's hook system (pre/post SQL, HTTP webhooks, checks) can be exposed
in sources.yml for advanced extraction workflows:

```yaml
# sources.yml (future feature)
- name: orders
  sling:
    mode: incremental
    primary_key: [id]
    update_key: modified_at
    hooks:
      pre:
        - type: query
          connection: source_postgres
          query: "ANALYZE public.orders"    # update stats before extraction
      post:
        - type: check
          check: "run.total_rows > 0"       # fail if no rows extracted
          on_failure: warn
```

## Error Handling

When a Sling extraction fails:
1. The extraction node is marked as FAILED in the DVT DAG
2. All downstream models that depend on this source are SKIPPED
3. The error message from Sling is surfaced in DVT's run results
4. `dvt retry` will re-attempt failed extraction nodes and their downstream models

For incremental extractions, failure is safe — the watermark was not updated,
so the next run will re-attempt the same range.

For CDC, Sling's own retry logic handles transient failures (configurable
`retry_attempts` and `retry_delay`).
