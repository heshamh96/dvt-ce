# DVT-CE Architecture

## What DVT Is

DVT is a cross-engine data transformation tool built on top of dbt-core.
It combines three proven technologies:

- **dbt-core** — DAG orchestration, SQL models, Jinja, ref(), source(), testing, docs, adapters
- **Sling** — high-performance data movement (EL) across 30+ databases, file systems, and cloud buckets
- **DuckDB** — lightweight analytics engine for multi-source extraction compute, `dvt show`, local dev, and virtual federation

The user experience is identical to dbt: write SQL models, declare sources, run `dvt run`.
The difference: DVT automatically handles cross-engine data movement so models can reference
sources on ANY database, and materialize results to ANY target (including cloud buckets).

## Core Principles

1. **dbt internals are NEVER modified.** DVT extends dbt via subclassing (DvtRunTask extends RunTask, DvtModelRunner extends ModelRunner). This allows rebasing on upstream dbt-core with minimal conflicts.

2. **The target database is the compute engine for pushdown models.** Models whose sources are all local push down to the target via dbt adapters. For extraction models with multiple remote sources, DuckDB is the compute engine (Sling streams sources → DuckDB → model SQL runs → Sling streams result to target). For single-source extraction, Sling streams directly from source to target.

3. **Sling handles ALL data streaming.** Cross-engine extraction, loading to targets, seeding CSV files, cross-target materialization, CDC — all via Sling. No custom JDBC code, no PySpark, no intermediate staging files managed by DVT.

4. **No hidden tables. Results land directly as model tables.** Users write standard dbt models and sources — no `connection` config on models, no `sling:` blocks anywhere. DVT automatically detects when extraction is needed by comparing `source.connection` (from sources.yml) vs `model.target` (from profiles.yml default or model config). When a mismatch is detected, DVT uses Sling Direct (single source → target model table) or DuckDB Compute (multiple sources → DuckDB → target model table). There is no `_dvt` staging schema and no hidden staging tables.

5. **Cross-engine incremental models work seamlessly.** dbt's `is_incremental()` macro works across engines. DVT pre-resolves the watermark value from the target, formats it as a dialect-specific literal for the source engine, and substitutes it into the extraction query before Sling executes it.

6. **DuckDB serves four purposes.** (1) Multi-source extraction compute — when a model references 2+ remote sources, DuckDB is the in-process compute engine. (2) `dvt show` — local queries without hitting the warehouse. (3) Local file/API processing. (4) Virtual federation (future). For pushdown and single-source extraction models, DuckDB is not involved.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DVT PROJECT                                   │
│                                                                         │
│  profiles.yml          sources.yml           models/                    │
│  ┌──────────────┐     ┌──────────────┐      ┌──────────────┐           │
│  │ target: sf   │     │ crm:         │      │ stg_customers │           │
│  │ outputs:     │     │  conn: pg    │      │  (standard)   │           │
│  │  sf: {...}   │     │  tables:     │      │ stg_orders    │           │
│  │  pg: {...}   │     │   -customers │      │  (incremental)│           │
│  │  s3: {...}   │     │   -orders    │      │ dim_customers │           │
│  │  gcs: {...}  │     │              │      │ fct_orders    │           │
│  └──────────────┘     └──────────────┘      │  target=s3    │           │
│                        (metadata only,       └──────────────┘           │
│                         no sling config)                                │
└─────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          DVT ENGINE                                     │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     DAG Resolution                               │   │
│  │                                                                   │   │
│  │  1. Parse sources.yml (metadata: connection + tables)             │   │
│  │  2. Parse models/ → detect source references                     │   │
│  │  3. Compare source.connection vs model.target for each model     │   │
│  │  4. Classify: pushdown / Sling Direct / DuckDB Compute          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Execution (per node in DAG order)            │   │
│  │                                                                   │   │
│  │  EXTRACTION — SLING DIRECT (single remote source/ref):              │   │
│  │    Sling streams source → model's named table on target           │   │
│  │    Supports incremental (Sling handles watermark + merge)         │   │
│  │    No hidden tables, result lands directly as model table         │   │
│  │                                                                   │   │
│  │  EXTRACTION — DUCKDB COMPUTE (multiple remote sources/refs):       │   │
│  │    Sling streams each source → DuckDB (in-memory)                 │   │
│  │    Model SQL runs in DuckDB (user wrote DuckDB SQL)               │   │
│  │    Sling streams result → model's named table on target           │   │
│  │    Table materialization only (no incremental)                     │   │
│  │                                                                   │   │
│  │  PUSHDOWN MODEL (all sources already on target):                  │   │
│  │    dbt adapter pushes down SQL to the target                      │   │
│  │    Standard dbt materialization (table/view/incremental/ephemeral)│   │
│  │                                                                   │   │
│  │  CROSS-TARGET MODEL (target != default):                          │   │
│  │    dbt adapter runs SQL on default target                         │   │
│  │    Sling streams result → model's configured target               │   │
│  │    Target may be another DB or a cloud bucket (Delta/Parquet/CSV) │   │
│  │                                                                   │   │
│  │  SEED NODE:                                                       │   │
│  │    Sling loads CSV → target (replaces dbt's slow Python seed)     │   │
│  │    Supports --full-refresh and --target overrides                 │   │
│  │                                                                   │   │
│  │  TEST NODE:                                                       │   │
│  │    Standard dbt test execution on the target                      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                               │
                    ┌──────────┼──────────┐
                    ▼          ▼          ▼
              ┌──────────┐ ┌───────┐ ┌──────────┐
              │ Databases│ │Buckets│ │  DuckDB  │
              │          │ │       │ │  (local)  │
              │Snowflake │ │  S3   │ │dvt show   │
              │Postgres  │ │  GCS  │ │dvt debug  │
              │BigQuery  │ │ Azure │ │local files│
              │Redshift  │ │       │ │           │
              │MySQL     │ │       │ │           │
              │...       │ │       │ │           │
              └──────────┘ └───────┘ └──────────┘
```

## Project Structure

```
core/
  dbt/                              # Upstream dbt-core 1.9.10 (UNTOUCHED)
    cli/
    task/
    parser/
    adapters/
    ...

  dvt/                              # DVT extension layer (NEW)
    __init__.py
    cli/
      __init__.py
      main.py                       # Click CLI: dvt run, dvt build, etc.
      commands.py                   # Command definitions
    tasks/
      __init__.py
      run.py                        # DvtRunTask(RunTask)
      build.py                      # DvtBuildTask(BuildTask)
      compile.py                    # DvtCompileTask(CompileTask)
      test.py                       # DvtTestTask(TestTask)
      seed.py                       # DvtSeedTask — Sling-based seeding
      sync.py                       # DvtSyncTask — environment bootstrap
      show.py                       # DvtShowTask — DuckDB local query
      debug.py                      # DvtDebugTask — multi-connection checks
    runners/
      __init__.py
      model_runner.py               # DvtModelRunner(ModelRunner)
      extraction_runner.py          # Runs Sling extraction when source.connection != model.target
      seed_runner.py                # Runs Sling seed loading
    extraction/
      __init__.py
      sling_client.py               # Python wrapper around Sling
      connection_mapper.py          # profiles.yml → Sling connection URLs
      watermark_formatter.py        # Dialect-specific watermark literal formatting
    loading/
      __init__.py
      sling_loader.py               # Load results to non-default targets via Sling
      bucket_materializer.py        # Delta/Parquet/CSV materialization to buckets
    config/
      __init__.py
      dvt_project.py                # DVT-specific project config
      target_resolver.py            # Resolves per-model target + connection overrides
    federation/
      __init__.py
      engine.py                     # DuckDB engine for dvt show / local dev
      duckdb_compute.py             # DuckDB extraction compute for multi-source models
      optimizer.py                  # Predicate/column pushdown to extraction queries
    sync/
      __init__.py
      adapter_installer.py          # pip install dbt-<adapter> from profiles.yml
      duckdb_extensions.py          # Install DuckDB extensions
      cloud_deps.py                 # Install cloud SDKs (boto3, google-cloud, etc.)
      sling_checker.py              # Verify Sling binary availability
```

## Integration Points with dbt-core

DVT hooks into dbt-core at these specific points:

1. **CLI entry point** — `dvt.cli.main:cli` is a separate Click group that mirrors
   dbt's commands but routes to DvtXxxTask subclasses.

2. **Task subclasses** — DvtRunTask(RunTask), DvtBuildTask(BuildTask), etc.
   Override `get_runner()` to return DVT runners. Reuse ALL dbt lifecycle decorators.

3. **Runner subclasses** — DvtModelRunner(ModelRunner). Override `execute()` with
   three-way dispatch: (a) pushdown — all sources local, delegate to parent;
   (b) Sling Direct — single remote source, Sling streams to model table;
   (c) DuckDB Compute — multiple remote sources, Sling → DuckDB → Sling → model table.
   For incremental single-source models, pre-resolve watermarks and format as
   dialect-specific literals before sending to Sling.

4. **Config extension** — DVT reads `target`, `format` from model config().
   Source `connection` metadata comes from sources.yml. These are passed through
   dbt's existing config system.

## Data Flow Examples

### Example 1: Simple pushdown (all sources on target)

```
Source: Snowflake.raw.customers (already on target)
Model: models/dim_customers.sql → config(materialized='table')
Target: Snowflake (default)

Flow:
  1. No extraction needed (source is on the target)
  2. Snowflake executes: CREATE TABLE dim_customers AS (SELECT ...)
  3. Done. This is stock dbt behavior.
```

### Example 2: Extraction (remote sources — Sling Direct)

```
User writes:
  sources.yml → crm source with connection: source_postgres (customers, orders tables)
  models/staging/stg_customers.sql → config(materialized='table')
    -- Written in DuckDB SQL (universal for single-source SELECT)
    SELECT * FROM {{ source('crm', 'customers') }}
  models/staging/stg_orders.sql → config(materialized='incremental', unique_key='id')
    -- Written in DuckDB SQL
    SELECT * FROM {{ source('crm', 'orders') }} {% if is_incremental() %} WHERE ...{% endif %}
  models/marts/fct_orders.sql → config(materialized='table')
    -- Written in Snowflake SQL (pushdown model — all refs are local)
    SELECT ... FROM {{ ref('stg_customers') }} JOIN {{ ref('stg_orders') }}

Target: Snowflake (default)

Flow:
  1. DVT detects source_postgres != Snowflake → extraction needed
  2. stg_customers: single remote source → Sling Direct
     Sling streams pg.customers → Snowflake.stg_customers (model table directly)
  3. stg_orders: single remote source, incremental → Sling Direct
     DVT pre-resolves watermark, Sling streams delta → Snowflake.stg_orders (merge)
  4. fct_orders: all refs local → Pushdown
     Snowflake executes SQL via adapter (standard dbt)
  5. Done. No hidden tables. No _dvt schema. User never configured Sling.
```

### Example 2b: Extraction (multiple remote sources — DuckDB Compute)

```
User writes:
  sources.yml → crm (connection: source_postgres), erp (connection: source_sqlserver)
  models/staging/stg_combined.sql → config(materialized='table')
    -- Written in DuckDB SQL (required for multi-source extraction)
    SELECT c.id, c.name, i.invoice_total
    FROM {{ source('crm', 'customers') }} c
    JOIN {{ source('erp', 'invoices') }} i ON c.id = i.customer_id

Target: Snowflake (default)

Flow:
  1. DVT detects 2 remote sources → DuckDB Compute (sub-path 3b)
  2. Sling streams pg.customers → DuckDB (in-memory)
  3. Sling streams mssql.invoices → DuckDB (in-memory)
  4. DuckDB executes model SQL (join)
  5. Sling streams result → Snowflake.stg_combined (model table directly)
  6. Done. No hidden tables. DuckDB was ephemeral.
```

### Example 3: Cross-target materialization to bucket

```
User writes:
  models/archive/archive_orders.sql → config(target='data_lake', format='delta')

Target: S3 bucket (model-level override)

Flow:
  1. Snowflake executes model SQL → temp result (refs stg_orders which already exists)
  2. Sling streams result from Snowflake → S3 as Delta Lake format
  3. Temp table dropped
  4. Done.
```

### Example 4: Seed via Sling

```
Seed: seeds/country_codes.csv
Target: Snowflake (default, or overridden via --target)

Flow:
  1. Sling loads CSV → Snowflake.public.country_codes
  2. Much faster than dbt's Python-based INSERT batching.
  3. Supports --full-refresh (truncate + reload).
```
