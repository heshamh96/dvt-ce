# DVT Configuration

## Overview

DVT extends dbt's configuration system with three additions:
1. **profiles.yml** — multi-adapter + bucket connections (extended from dbt)
2. **sources.yml** — Sling extraction config per source table (extended from dbt)
3. **Model config** — per-model target override + bucket format (extended from dbt)

All standard dbt configuration (dbt_project.yml, selectors.yml, packages.yml) works unchanged.

## profiles.yml

DVT extends profiles.yml to support multiple adapter types per profile AND cloud bucket
connections as first-class targets.

```yaml
my_project:
  target: prod_snowflake                     # default target for dvt run
  outputs:

    # === Database Targets ===

    prod_snowflake:                           # default target
      type: snowflake
      account: xyz123.us-east-1
      user: dvt_service
      password: "{{ env_var('SNOWFLAKE_PASSWORD') }}"
      role: TRANSFORMER
      database: ANALYTICS
      warehouse: TRANSFORM_WH
      schema: PUBLIC
      threads: 8

    staging_postgres:                         # another database target
      type: postgres
      host: staging.internal
      port: 5432
      user: dvt_user
      password: "{{ env_var('PG_PASSWORD') }}"
      dbname: staging
      schema: public
      threads: 4

    # === Source Connections (read-only, used by extraction) ===

    source_postgres:                          # source database
      type: postgres
      host: prod-pg.internal
      port: 5432
      user: readonly
      password: "{{ env_var('SOURCE_PG_PASSWORD') }}"
      dbname: production
      schema: public

    source_mysql:                             # another source
      type: mysql
      host: mysql.internal
      port: 3306
      user: reader
      password: "{{ env_var('MYSQL_PASSWORD') }}"
      database: billing

    # === Bucket Targets ===

    data_lake:                                # S3 bucket as target
      type: s3
      bucket: company-data-lake
      region: us-east-1
      access_key_id: "{{ env_var('AWS_ACCESS_KEY_ID') }}"
      secret_access_key: "{{ env_var('AWS_SECRET_ACCESS_KEY') }}"
      prefix: analytics/                     # base path within bucket

    archive_gcs:                              # GCS bucket as target
      type: gcs
      project: my-gcp-project
      bucket: archive-data
      credentials_json: "{{ env_var('GCP_CREDENTIALS_JSON') }}"
      prefix: archive/

    landing_azure:                            # Azure Blob as target
      type: azure
      account: mystorageaccount
      container: raw-data
      access_key: "{{ env_var('AZURE_ACCESS_KEY') }}"
```

### Connection Types

**Database types** (require dbt adapter):
`postgres`, `snowflake`, `bigquery`, `redshift`, `mysql`, `sqlserver`,
`databricks`, `oracle`, `trino`, `clickhouse`, `duckdb`, etc.

**Bucket types** (require cloud SDK):
`s3`, `gcs`, `azure`

**File types** (no external dependencies):
`local` — local filesystem path

### How connections are used

| Connection role | How DVT uses it |
|----------------|----------------|
| Default target | dbt adapter pushes down model SQL |
| Source connection | Sling extracts data to default target |
| Alternate target (DB) | Sling loads model results to this DB |
| Alternate target (bucket) | Sling writes model results as Delta/Parquet/CSV |

## sources.yml

DVT extends dbt's sources.yml with a `sling:` configuration block that controls
how each source table is extracted to the default target.

```yaml
version: 2

sources:
  - name: crm
    description: "CRM system on PostgreSQL"
    connection: source_postgres               # profiles.yml output name
    schema: public
    sling:                                    # source-level defaults
      mode: incremental
      primary_key: [id]
    tables:
      - name: customers
        description: "Customer master data"
        sling:                                # table-level overrides
          update_key: updated_at
        columns:
          - name: id
            data_tests:
              - unique
              - not_null

      - name: orders
        sling:
          update_key: modified_at
          select: [id, customer_id, order_date, total_amount, modified_at]  # column selection

      - name: ref_countries
        sling:
          mode: full-refresh                  # small reference table, always full

      - name: events
        sling:
          mode: change-capture                # CDC from transaction log
          change_capture_options:
            run_max_events: 10000
            soft_delete: true

  - name: billing
    description: "Billing system on MySQL"
    connection: source_mysql
    schema: billing
    sling:
      mode: incremental
      primary_key: [invoice_id]
      update_key: last_modified
    tables:
      - name: invoices
      - name: payments
      - name: line_items
        sling:
          mode: full-refresh
          primary_key: [line_item_id]

  - name: files
    description: "CSV files on S3"
    connection: data_lake
    sling:
      mode: full-refresh
    tables:
      - name: exchange_rates
        sling:
          src_stream: "s3://company-data-lake/reference/exchange_rates.csv"
          source_options:
            format: csv
            header: true
```

### Sling Config Keys (per source table)

| Key | Description | Default |
|-----|-------------|---------|
| `mode` | Extraction mode: `full-refresh`, `incremental`, `truncate`, `snapshot`, `change-capture` | `full-refresh` |
| `primary_key` | Column(s) for upsert in incremental mode | none |
| `update_key` | Column for watermark-based incremental extraction | none |
| `select` | List of columns to extract (column pruning) | all columns |
| `src_stream` | Custom SQL or file path override for extraction | table name |
| `source_options` | Sling source options (format, encoding, flatten, etc.) | {} |
| `target_options` | Sling target options (column_casing, batch_limit, etc.) | {} |
| `change_capture_options` | CDC-specific options (run_max_events, soft_delete, etc.) | {} |
| `transforms` | Sling inline transforms (trim, upper, hash, etc.) | none |
| `disabled` | Skip this table during extraction | false |

### Config Hierarchy

```
dbt_project.yml (project-level DVT defaults)
  → source-level sling: {} (applies to all tables in this source)
    → table-level sling: {} (overrides source defaults)
```

Example in dbt_project.yml:
```yaml
# dbt_project.yml
name: my_project
...

# DVT-specific config
dvt:
  extraction_schema: dvt_raw          # schema for extraction tables (default: dvt_raw)
  sling:                              # project-level extraction defaults
    mode: full-refresh
    target_options:
      column_casing: snake
```

## Model Config

DVT extends dbt's model config with `target` and `format` options.

### target (per-model target override)

```sql
-- models/archive_orders.sql
{{ config(
    materialized='table',
    target='data_lake',              -- materialize to S3 instead of default target
    format='delta',                  -- output format for bucket targets
    path='analytics/orders/',        -- path within the bucket
) }}

SELECT * FROM {{ ref('fct_orders') }}
WHERE order_date < DATEADD(year, -2, CURRENT_DATE)
```

### format (bucket materialization format)

Only applies when `target` is a bucket connection. Options:
- `delta` (default) — Delta Lake format
- `parquet` — plain Parquet files
- `csv` — CSV files
- `json` — JSON files
- `jsonlines` — JSON Lines files

### Standard dbt config (unchanged)

All standard dbt config works as-is:
- `materialized`: table, view, incremental, ephemeral, snapshot
- `schema`: target schema override
- `database`: target database override
- `tags`: model tags for selection
- `pre_hook` / `post_hook`: SQL hooks
- `unique_key`: for incremental merge
- `strategy`: for snapshots (timestamp, check)
- etc.

### Config in dbt_project.yml

```yaml
# dbt_project.yml
name: my_project
version: '0.1.2'
profile: 'my_project'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]
analysis-paths: ["analyses"]
macro-paths: ["macros"]

models:
  my_project:
    staging:
      +materialized: view
    marts:
      +materialized: table
    archive:
      +materialized: table
      +target: data_lake           # all archive models go to S3
      +format: delta

seeds:
  my_project:
    +schema: seeds

# DVT-specific project config
dvt:
  extraction_schema: dvt_raw
```

## Extraction Node Naming

When DVT generates extraction nodes from sources.yml, they are named:

```
{extraction_schema}.{source_name}__{table_name}

Examples:
  dvt_raw.crm__customers
  dvt_raw.crm__orders
  dvt_raw.billing__invoices
```

The extraction schema is configurable via `dvt.extraction_schema` in `dbt_project.yml`.
Default: `dvt_raw`.

In model SQL, users reference sources normally via `{{ source('crm', 'customers') }}`.
DVT resolves this to `dvt_raw.crm__customers` (the extraction table) instead of
the original source table on the remote database.

## Environment Variables

DVT respects all dbt environment variables plus:

| Variable | Description |
|----------|-------------|
| `DVT_PROFILES_DIR` | Override profiles.yml location |
| `DVT_EXTRACTION_SCHEMA` | Override extraction schema name |
| `SLING_THREADS` | Number of parallel Sling extractions |
| `SLING_STATE` | State store for CDC (Sling native) |
| `DUCKDB_PATH` | Path to DuckDB binary (for extensions) |
