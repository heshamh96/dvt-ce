# DVT Execution Paths

## Overview

Every node in the DVT DAG follows one of four execution paths, selected automatically
based on the node type and its configuration.

## Path 1: Extraction (Source → Target via Sling)

**When:** A source table defined in `sources.yml` needs to be available on the target.

**What happens:**
1. DVT generates an extraction node for each source table
2. The extraction node becomes an upstream dependency of any model that refs the source
3. At execution time, DVT calls Sling to stream data from the source connection to the default target
4. The result is a physical table on the target (e.g., `dvt_raw.crm__customers`)
5. On subsequent runs, Sling respects the extraction mode:
   - `full-refresh` → DROP + CREATE (or TRUNCATE + INSERT)
   - `incremental` → only rows where `update_key > last_watermark` are extracted and upserted
   - `change-capture` → reads the source DB's transaction log for inserts/updates/deletes

**Sling call:**
```python
sling.run(
    src_conn=source_connection_url,          # from profiles.yml via connection mapper
    src_stream=extraction_query,             # table name or optimized SELECT with pushdown
    tgt_conn=target_connection_url,          # default target from profiles.yml
    tgt_object="dvt_raw.crm__customers",     # extraction schema + source name
    mode="incremental",                      # from sources.yml sling config
    primary_key=["id"],                      # from sources.yml sling config
    update_key="updated_at",                 # from sources.yml sling config
)
```

**Extraction node naming convention:**
```
{extraction_schema}.{source_name}__{table_name}

Example:
  source: crm, table: customers → dvt_raw.crm__customers
  source: billing, table: invoices → dvt_raw.billing__invoices
```

The extraction schema (`dvt_raw` by default) is configurable in `dbt_project.yml`.

**Extraction node in the DAG:**
```
[dvt_raw.crm__customers] ──→ [dim_customers] ──→ [test: unique_customer_id]
[dvt_raw.crm__orders]    ──→ [fct_orders]    ──→ [test: not_null_order_id]
```

These nodes are visible in `dvt docs`, `dvt ls`, and lineage graphs.
They show the extraction mode, last run timestamp, and row count.

## Path 2: Pushdown (Model SQL → Target via dbt Adapter)

**When:** A model's SQL runs on the default target where all its sources are already cached.

**What happens:**
1. dbt compiles the model (Jinja → SQL)
2. The `{{ source('crm', 'customers') }}` ref resolves to `dvt_raw.crm__customers`
   (the extraction node's physical table on the target)
3. The dbt adapter executes the SQL on the target database
4. Materialization is handled by the adapter's macros (CREATE TABLE AS, MERGE INTO, CREATE VIEW, etc.)

**This is stock dbt behavior.** DVT's DvtModelRunner delegates to the parent ModelRunner
when the model's target matches the default target. No Sling involved, no DuckDB involved.

```python
class DvtModelRunner(ModelRunner):
    def execute(self, model, manifest):
        if self.is_pushdown(model):
            return super().execute(model, manifest)  # stock dbt
        else:
            return self.execute_cross_target(model, manifest)
```

## Path 3: Cross-Target Materialization (Model → Different Target via Sling)

**When:** A model has `config(target='other_db')` or `config(target='s3_bucket')`.

**What happens:**
1. The model SQL still runs on the DEFAULT target (where sources are cached) via pushdown
2. The result is written to a temporary table on the default target
3. Sling streams the result FROM the default target TO the model's configured target
4. If the target is a database: Sling creates/upserts the table
5. If the target is a bucket: Sling writes in the configured format (Delta, Parquet, CSV, etc.)
6. The temporary table on the default target is dropped

**Flow:**
```
Default target (Snowflake)           Model target (S3)
┌──────────────────────────┐        ┌──────────────────────────┐
│ 1. Execute model SQL     │        │                          │
│    → temp_result table   │───Sling──→ 3. Write as Delta     │
│ 2. Sling reads result    │        │    s3://bucket/path/     │
│ 4. Drop temp_result      │        │                          │
└──────────────────────────┘        └──────────────────────────┘
```

**Bucket materialization formats:**
- `delta` (default for buckets) — Delta Lake format, supports time travel and incremental
- `parquet` — plain Parquet files
- `csv` — CSV files
- `json` / `jsonlines` — JSON formats

**Model config:**
```sql
{{ config(
    materialized='table',
    target='data_lake',           -- profiles.yml output name
    format='delta',               -- output format for bucket targets
    path='analytics/orders/',     -- path within the bucket
) }}
```

## Path 4: Local Query (dvt show — DuckDB)

**When:** User runs `dvt show --select model_name` or `dvt show --inline "SELECT ..."`.

**What happens:**
1. DuckDB starts in-process (ephemeral, in-memory)
2. For sources that support ATTACH (Postgres, MySQL, SQLite): DuckDB ATTACHes directly
3. For other sources: DuckDB reads cached extraction data or uses scanner extensions
4. Model SQL is transpiled from target dialect to DuckDB SQL via SQLGlot (if needed)
5. Query executes locally in DuckDB
6. Results displayed to terminal
7. No materialization, no data written anywhere

**This is for development and ad-hoc exploration only.** It does NOT replace `dvt run`.

```bash
# Show a model's output locally
dvt show --select dim_customers --limit 100

# Run inline SQL locally
dvt show --inline "SELECT COUNT(*) FROM {{ source('crm', 'customers') }}"
```

## Path 5: Seed Loading (dvt seed — Sling)

**When:** User runs `dvt seed` to load CSV files from the `seeds/` directory.

**What happens:**
1. DVT discovers CSV files in the seed paths
2. For each seed, Sling streams the CSV directly to the target database
3. Much faster than dbt's default Python-based INSERT batching
4. Supports `--full-refresh` (DROP + CREATE) and default incremental append
5. Supports `--target <target_name>` to redirect seeds to a specific target

**Sling call for seeds:**
```python
sling.run(
    src_stream=f"file://{seed_csv_path}",
    tgt_conn=target_connection_url,
    tgt_object=f"{schema}.{seed_name}",
    mode="full-refresh",  # or "truncate" based on flags
)
```

**Why Sling for seeds:**
- dbt's default seed loader uses Python `agate` library + batch INSERTs
- For a 100K row CSV, dbt takes minutes; Sling takes seconds
- Sling uses native bulk loading (COPY for Postgres, COPY INTO for Snowflake, bcp for SQL Server, etc.)
- Sling handles type inference, encoding, and schema evolution automatically

## Execution Path Selection Logic

```python
def resolve_execution_path(node, manifest, config):
    """Determine which path a node takes."""

    if node.resource_type == "source":
        return "extraction"

    if node.resource_type == "seed":
        return "seed_sling"

    if node.resource_type == "test":
        return "pushdown"  # tests always run on the target

    if node.resource_type == "model":
        model_target = node.config.get("target", config.default_target)
        if model_target == config.default_target:
            return "pushdown"
        else:
            return "cross_target"
```

## Summary Table

| Path | Node Type | Data Movement | Compute Engine | Sling | DuckDB |
|------|-----------|---------------|----------------|-------|--------|
| Extraction | source | source → target | N/A (EL only) | YES | no |
| Pushdown | model | none | target DB | no | no |
| Cross-target | model | target → other target/bucket | target DB | YES | no |
| Local query | dvt show | streamed to memory | DuckDB | no | YES |
| Seed loading | seed | CSV → target | N/A (EL only) | YES | no |
