# DuckDB Role in DVT

## Scope

DuckDB is NOT DVT's core compute engine. The target database (via dbt adapters)
handles model execution. DuckDB serves three specific, scoped purposes.

## Purpose 1: dvt show (Local Development)

**What:** Run model queries locally without hitting the warehouse.

**How:**
1. DuckDB starts in-process (ephemeral, in-memory)
2. Sources are accessed via:
   - DuckDB ATTACH (for Postgres, MySQL, SQLite families)
   - DuckDB scanner extensions (postgres_scanner, mysql_scanner)
   - Direct file reading (Parquet, CSV, JSON on local filesystem or S3/GCS)
3. Model SQL executes in DuckDB
4. Results displayed in terminal
5. DuckDB instance destroyed after query completes

**When SQLGlot is needed:**
If the model SQL is written in a target-specific dialect (e.g., Snowflake SQL),
SQLGlot transpiles it to DuckDB SQL before execution. This handles:
- Function name differences (DATE_TRUNC syntax, NVL vs COALESCE, etc.)
- Quoting style differences
- Type casting syntax

**Example:**
```bash
# Show model output locally
dvt show --select dim_customers --limit 50

# Run inline SQL locally with source references
dvt show --inline "SELECT COUNT(*) FROM {{ source('crm', 'customers') }}"

# Output as CSV
dvt show --select fct_orders --output csv > orders.csv
```

## Purpose 2: Local File and API Processing

**What:** Process local files (CSV, Parquet, JSON) and APIs as sources.

**How:**
When a source connection is a local filesystem or a cloud bucket,
DuckDB reads the files directly using its native readers:

```sql
-- DuckDB can read these natively:
SELECT * FROM 'data/customers.csv'
SELECT * FROM 'data/*.parquet'
SELECT * FROM 's3://bucket/path/*.json'    -- via httpfs extension
SELECT * FROM delta_scan('s3://bucket/delta_table/')  -- via delta extension
```

This is useful when:
- Source data is in files, not databases
- You want to prototype models against local data
- You're working with API responses saved as JSON

**Note:** For production runs (`dvt run`), file-based sources are still
extracted to the target via Sling. DuckDB is only used for `dvt show`
and local development.

## Purpose 3: Virtual Federation (Future)

**What:** Ephemeral cross-source queries without materializing to the target.

**How:**
A future materialization type (`materialized='virtual'` or `materialized='federated'`)
would execute the model in DuckDB instead of pushing down to the target.
DuckDB ATTACHes to multiple sources and runs the join locally.
The result is NOT materialized anywhere — it's computed on-demand.

This is the Denodo-style pattern: data stays in place, query goes to the data.

**When this is useful:**
- Real-time dashboards that need fresh cross-source data
- Ad-hoc exploration across multiple databases
- Small-volume cross-source queries where extraction overhead isn't worth it

**This is NOT a P0/P1 feature.** It's a future enhancement (P4).

## DuckDB Extensions

`dvt sync` installs DuckDB extensions based on the connections in profiles.yml:

| profiles.yml type | DuckDB Extension |
|-------------------|-----------------|
| postgres | postgres_scanner |
| mysql | mysql_scanner |
| sqlite | sqlite_scanner |
| s3 | httpfs |
| gcs | httpfs |
| azure | azure |
| any with Delta format | delta |
| any with Parquet | (built-in) |
| any with JSON | json |
| any with Excel | spatial (for xlsx) |

Extensions are installed to DuckDB's extension directory (usually `~/.duckdb/extensions/`).

## SQLGlot Role

SQLGlot is used ONLY in the context of DuckDB:

1. **dvt show transpilation** — when model SQL is in a target dialect (Snowflake, BigQuery,
   Redshift, etc.) and needs to run locally in DuckDB, SQLGlot transpiles it.

2. **Extraction query generation** (future, P4) — when the federation optimizer
   generates source-side queries, SQLGlot ensures they're valid for the source's dialect.

SQLGlot is NOT used in the core `dvt run` pipeline. Model SQL is written in the
target's dialect and pushed down unchanged via the dbt adapter.

## Configuration

DuckDB configuration in dbt_project.yml (optional):

```yaml
# dbt_project.yml
dvt:
  duckdb:
    memory_limit: "4GB"          # max memory for dvt show
    threads: 4                   # parallel threads
    temp_directory: "/tmp/dvt"   # spill directory
```

## Dependencies

- `duckdb>=0.9.0` — in setup.py install_requires
- `sqlglot>=20.0.0` — in setup.py install_requires (for transpilation)
- DuckDB extensions — installed by `dvt sync`, NOT bundled in the package
