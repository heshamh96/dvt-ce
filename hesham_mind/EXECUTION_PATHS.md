# DVT Execution Paths

## Overview

Every model in the DVT DAG follows one of three execution paths, with the extraction
path having two sub-paths. DVT determines the path automatically based on where
the model's sources live relative to its target. Seeds and tests have their own fixed paths.

### Three Execution Paths

| Path | Condition | Engine |
|------|-----------|--------|
| **Default Pushdown** | All sources on default target | Target DB via adapter |
| **Non-Default Pushdown** | All sources on non-default target (same as model target) | Non-default target DB via adapter |
| **Extraction** | Source(s) on different connection than model target | Sling Direct or DuckDB Compute |

### Two Extraction Sub-Paths

| Sub-Path | Condition | How It Works |
|----------|-----------|-------------|
| **Sling Direct (3a)** | Exactly 1 remote source/ref | Sling streams source → model table on target |
| **DuckDB Compute (3b)** | 2+ remote sources/refs | Sling → DuckDB → model SQL → Sling → model table on target |

## Path 1: Extraction

**When:** A model references source(s) whose `connection` (from sources.yml) differs from the model's target (from profiles.yml default or model config).

**User-facing rule:** All extraction models must be written in **DuckDB SQL** syntax.

### Sub-Path 3a: Sling Direct (Single Remote Source)

**Condition:** Model references exactly ONE remote `source()` or `ref()`.

**What happens:**
1. DVT detects that the single `source()` or `ref()` is on a remote connection
2. Sling streams data directly from the source to the model's named table on the target
3. No intermediate tables, no DuckDB, no staging schema
4. Result lands directly as the model's table (e.g., `stg_orders`)

**Sling call (generated internally by DVT):**
```python
# DVT generates this automatically — the user never sees or configures it
sling.run(
    src_conn=source_connection_url,          # from source's `connection` in sources.yml
    src_stream=f"SELECT * FROM {source_schema}.{source_table}",  # auto-generated
    tgt_conn=target_connection_url,          # model's target from profiles.yml
    tgt_object=f"{target_schema}.{model_name}",  # model's named table directly
    mode="full-refresh",                     # for table materialization
)
```

**Supports incremental:**
For `incremental` models with a single remote source:
```
1. DVT queries target: SELECT MAX(updated_at) FROM target_schema.stg_orders
2. Gets raw value: datetime(2024, 3, 14, 12, 0, 0)
3. Formats for source dialect:
   - PostgreSQL: '2024-03-14 12:00:00.000000'::TIMESTAMP
   - Oracle:     TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')
   - SQL Server: CONVERT(DATETIME2, '2024-03-14 12:00:00.000000', 121)
4. Sling extraction query filtered by watermark
5. Sling merges delta directly into model's table on target
```

**Incremental strategy mapping:**

| dbt Strategy | Sling Behavior |
|---|---|
| `append` | Sling `incremental` mode, no primary_key (append-only insert) |
| `merge` | Sling `incremental` mode with primary_key (upsert: update + insert) |
| `delete+insert` | Sling `incremental` mode with `merge_strategy: delete_insert` |
| `insert_overwrite` | Sling partition overwrite via target_options |

**Materialization constraints (Sling Direct):**

| Materialization | Supported? |
|---|---|
| `table` | Yes |
| `incremental` | **Yes** (Sling handles watermark + merge) |
| `view` | Coerced to table (DVT001 warning) |
| `ephemeral` | Not supported (DVT110 error) |

**`--full-refresh` behavior:**
- `is_incremental()` returns false → no watermark filter
- Sling re-extracts full source table → model table on target
- Standard dbt behavior

**Example model (single remote source, incremental):**
```sql
-- models/staging/stg_orders.sql
-- EXTRACTION MODEL: single remote source → Sling Direct
-- Written in DuckDB SQL (universal enough for Sling to execute on source)
{{ config(
    materialized='incremental',
    unique_key='id'
) }}
SELECT id, customer_id, order_date, total, updated_at
FROM {{ source('crm', 'orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

### Sub-Path 3b: DuckDB Compute (Multiple Remote Sources)

**Condition:** Model references 2+ remote `source()` and/or `ref()`.

**What happens:**
1. DVT detects that multiple sources/refs are on remote connections
2. For each remote source, Sling streams data into DuckDB (in-memory)
3. Model SQL executes in DuckDB (user wrote it in DuckDB SQL)
4. Sling streams the result from DuckDB to the model's named table on the target
5. DuckDB instance is destroyed (ephemeral)
6. No intermediate tables on the target

**Flow:**
```
Source A (Postgres)     Source B (SQL Server)
    │                       │
    │ Sling                 │ Sling
    ▼                       ▼
┌────────────────────────────────┐
│         DuckDB (in-memory)     │
│                                │
│  Model SQL runs here           │
│  (user wrote DuckDB SQL)       │
│                                │
│  Result: joined/transformed    │
└────────────────────────────────┘
                │
                │ Sling
                ▼
┌────────────────────────────────┐
│    Target (e.g., Snowflake)    │
│    model's named table         │
└────────────────────────────────┘
```

**Materialization constraints (DuckDB Compute):**

| Materialization | Supported? |
|---|---|
| `table` | Yes |
| `incremental` | **No** — DVT112 error |
| `view` | Coerced to table (DVT001 warning) |
| `ephemeral` | Not supported (DVT110 error) |

**Example model (multiple remote sources):**
```sql
-- models/staging/stg_combined.sql
-- EXTRACTION MODEL: multiple remote sources → DuckDB Compute
-- Written in DuckDB SQL (DuckDB is the compute engine)
-- Must use table materialization (incremental not supported)
{{ config(materialized='table') }}
SELECT c.id, c.name, c.email, i.invoice_total, i.invoice_date
FROM {{ source('crm', 'customers') }} c
JOIN {{ source('erp', 'invoices') }} i ON c.id = i.customer_id
WHERE i.invoice_date >= '2024-01-01'
```

**DvtModelRunner logic:**
```python
class DvtModelRunner(ModelRunner):
    def execute(self, model, manifest):
        model_target = model.config.get("target", self.default_target)

        # Count remote sources/refs
        remote_sources = self.detect_remote_sources(model, manifest)

        if not remote_sources:
            # Path 1/2: Pushdown — all sources local
            if model_target != self.default_target:
                return self.execute_cross_target(model, manifest)
            else:
                return super().execute(model, manifest)  # standard dbt pushdown

        elif len(remote_sources) == 1:
            # Sub-path 3a: Sling Direct — single remote source
            return self.execute_sling_direct(model, manifest, remote_sources[0])

        else:
            # Sub-path 3b: DuckDB Compute — multiple remote sources
            if model.config.get("materialized") == "incremental":
                raise DVT112Error(model.name)
            return self.execute_duckdb_compute(model, manifest, remote_sources)

    def detect_remote_sources(self, model, manifest):
        """For each source()/ref() in model, check if connection != model.target"""
        model_target = model.config.get("target", self.default_target)
        remote = []
        for source_ref in model.source_references:
            source_conn = manifest.sources[source_ref].connection
            if source_conn != model_target:
                remote.append(source_ref)
        return remote
```

## Path 2: Pushdown (Default & Non-Default)

### Default Pushdown

**When:** A model's sources are all on the same connection as its target, and its target is the default target.

**What happens:**
1. dbt compiles the model (Jinja → SQL)
2. `{{ ref('stg_customers') }}` resolves to the physical table on the target
3. The dbt adapter executes the SQL on the target database
4. Materialization is handled by the adapter's macros (CREATE TABLE AS, MERGE INTO, etc.)

**This is stock dbt behavior.** No Sling, no DuckDB. User writes in target dialect.

### Non-Default Pushdown

**When:** Model targets a non-default adapter (e.g., `config(target='mysql_prod')`) but ALL its sources are on that same non-default target.

**What happens:**
1. DVT uses the non-default adapter instead of the global default
2. Pure pushdown — SQL runs on the non-default target
3. No Sling, no DuckDB
4. User writes in that target's dialect

**Example:**
```sql
-- models/marts/dim_customers.sql (all refs on target — standard pushdown)
-- Written in target dialect (e.g., Snowflake SQL)
{{ config(materialized='table') }}
SELECT
    c.id, c.name, c.email,
    COUNT(o.id) AS order_count
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
GROUP BY 1, 2, 3
```

## Path 3: Cross-Target Materialization (Model → Different Target via Sling)

**When:** A model has `config(target='other_db')` or `config(target='s3_bucket')`
that differs from the default target.

**What happens:**
1. The model SQL runs on the DEFAULT target via adapter pushdown
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
3. For other sources: DuckDB uses scanner extensions
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
4. Default: truncate + insert. With `--full-refresh`: drop + create + load.
5. Supports `--target <target_name>` to redirect seeds to a specific target

**Why Sling for seeds:**
- dbt's default seed loader uses Python `agate` library + batch INSERTs
- For a 100K row CSV, dbt takes minutes; Sling takes seconds
- Sling uses native bulk loading (COPY for Postgres, COPY INTO for Snowflake, bcp for SQL Server)
- Sling handles type inference, encoding, and schema evolution automatically

## Execution Path Selection Logic

```python
def resolve_execution_path(node, manifest, config):
    """Determine which path a node takes."""

    if node.resource_type == "seed":
        return "seed_sling"

    if node.resource_type == "test":
        return "pushdown"  # tests always run on the target

    if node.resource_type == "model":
        model_target = node.config.get("target", config.default_target)

        # Count remote source references
        remote_sources = [
            src for src in node.source_references
            if manifest.sources[src].connection != model_target
        ]

        if remote_sources:
            if len(remote_sources) == 1:
                return "extraction_sling_direct"   # Sub-path 3a
            else:
                return "extraction_duckdb_compute"  # Sub-path 3b

        # Cross-target: model target differs from default (but all sources local)
        if model_target != config.default_target:
            return "pushdown_non_default"  # Non-default pushdown

        # Default: pushdown via adapter
        return "pushdown_default"
```

## Summary Table

| Path | Trigger | Data Movement | Compute Engine | Sling | DuckDB |
|------|---------|---------------|----------------|-------|--------|
| Default Pushdown | all sources on default target | none | Target DB (adapter) | no | no |
| Non-Default Pushdown | all sources on non-default target | none | Non-default target DB (adapter) | no | no |
| Sling Direct (3a) | 1 remote source/ref | source → model table on target | Sling (stream) | YES | no |
| DuckDB Compute (3b) | 2+ remote sources/refs | sources → DuckDB → model table on target | DuckDB (in-process) + Sling (stream) | YES | YES |
| Cross-target output | target is bucket or other DB | result → other target/bucket | Target DB (adapter) + Sling (move) | YES | no |
| Local query | `dvt show` | streamed to memory | DuckDB | no | YES |
| Seed loading | `dvt seed` | CSV → target | Sling (bulk load) | YES | no |
