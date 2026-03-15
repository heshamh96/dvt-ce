# DVT-CE Architecture

## What DVT Is

DVT is a cross-engine data transformation tool built on top of dbt-core.
It combines three proven technologies:

- **dbt-core** — DAG orchestration, SQL models, Jinja, ref(), source(), testing, docs, adapters
- **Sling** — high-performance data movement (EL) across 30+ databases, file systems, and cloud buckets
- **DuckDB** — lightweight local analytics engine for `dvt show`, local dev, and virtual federation

The user experience is identical to dbt: write SQL models, declare sources, run `dvt run`.
The difference: DVT automatically handles cross-engine data movement so models can reference
sources on ANY database, and materialize results to ANY target (including cloud buckets).

## Core Principles

1. **dbt internals are NEVER modified.** DVT extends dbt via subclassing (DvtRunTask extends RunTask, DvtModelRunner extends ModelRunner). This allows rebasing on upstream dbt-core with minimal conflicts.

2. **The target database is the compute engine.** Models push down to the target via dbt adapters. DVT does NOT run model SQL in DuckDB (except for `dvt show`). The warehouse does the heavy lifting.

3. **Sling handles ALL data movement.** Extraction from sources, loading to targets, seeding CSV files, cross-target materialization, CDC — all via Sling. No custom JDBC code, no PySpark, no intermediate staging files managed by DVT.

4. **Sources become physical extraction nodes in the DAG.** When `dvt run` is issued, each source table becomes a real DAG node that Sling materializes on the target. These are cached — incremental sources only extract the delta on subsequent runs.

5. **DuckDB is scoped to three use cases.** `dvt show` (local queries), local file/API processing, and virtual federation. It is NOT the core transform engine.

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           DVT PROJECT                                   │
│                                                                         │
│  profiles.yml          sources.yml           models/                    │
│  ┌──────────────┐     ┌──────────────┐      ┌──────────────┐           │
│  │ target: sf   │     │ crm:         │      │ dim_customer  │           │
│  │ outputs:     │     │  conn: pg    │      │ fct_orders    │           │
│  │  sf: {...}   │     │  sling:      │      │ rpt_revenue   │           │
│  │  pg: {...}   │     │   mode: incr │      │               │           │
│  │  s3: {...}   │     │   pk: [id]   │      │ config:       │           │
│  │  gcs: {...}  │     │   uk: upd_at │      │  target=s3    │           │
│  └──────────────┘     └──────────────┘      └──────────────┘           │
└─────────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          DVT ENGINE                                     │
│                                                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     DAG Resolution                               │   │
│  │                                                                   │   │
│  │  1. Parse sources.yml → generate extraction nodes                │   │
│  │  2. Parse models/ → standard dbt parsing                         │   │
│  │  3. Build DAG: extraction nodes → models → tests                 │   │
│  │  4. Resolve targets: per-model target overrides                  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                               │                                         │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                     Execution (per node in DAG order)            │   │
│  │                                                                   │   │
│  │  EXTRACTION NODE:                                                 │   │
│  │    Sling streams data from source connection → default target     │   │
│  │    Mode: full-refresh / incremental / change-capture              │   │
│  │    Result: physical table on the default target                   │   │
│  │                                                                   │   │
│  │  MODEL NODE (target == default target):                           │   │
│  │    dbt adapter pushes down SQL to the target                      │   │
│  │    Standard dbt materialization (table/view/incremental/ephemeral)│   │
│  │                                                                   │   │
│  │  MODEL NODE (target != default target):                           │   │
│  │    dbt adapter runs SQL on default target (where sources live)    │   │
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
      extraction_runner.py          # Runs Sling extraction for source nodes
      seed_runner.py                # Runs Sling seed loading
    extraction/
      __init__.py
      sling_client.py               # Python wrapper around Sling
      connection_mapper.py          # profiles.yml → Sling connection URLs
      node_generator.py             # Auto-generates extraction DAG nodes from sources.yml
    loading/
      __init__.py
      sling_loader.py               # Load results to non-default targets via Sling
      bucket_materializer.py        # Delta/Parquet/CSV materialization to buckets
    config/
      __init__.py
      dvt_project.py                # DVT-specific project config
      source_config.py              # Sling extraction config from sources.yml
      target_resolver.py            # Resolves per-model target overrides
    federation/
      __init__.py
      engine.py                     # DuckDB engine for dvt show / local dev
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
   Override `get_runner()` to return DVT runners. Override `before_run()` to
   inject extraction nodes. Reuse ALL dbt lifecycle decorators.

3. **Runner subclasses** — DvtModelRunner(ModelRunner). Override `execute()` to
   handle cross-target materialization. Override `compile()` for extraction queries.

4. **Manifest augmentation** — After dbt builds the manifest, DVT injects
   extraction nodes (one per source table) as upstream dependencies.

5. **Config extension** — DVT reads `sling:` config from sources.yml and
   `target:` / `format:` from model config(). These are passed through dbt's
   existing config system.

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

### Example 2: Cross-engine extraction + pushdown

```
Source: Postgres.public.customers (remote)
Source: Postgres.public.orders (remote)
Model: models/fct_orders.sql → config(materialized='incremental')
Target: Snowflake (default)

Flow:
  1. Sling extracts customers → Snowflake.dvt_raw.crm__customers (incremental)
  2. Sling extracts orders → Snowflake.dvt_raw.crm__orders (incremental)
  3. Snowflake executes: MERGE INTO fct_orders ... (pushdown via dbt-snowflake)
  4. Done.
```

### Example 3: Cross-target materialization to bucket

```
Source: Postgres.public.orders (remote)
Model: models/archive_orders.sql → config(target='data_lake', format='delta')
Target: S3 bucket

Flow:
  1. Sling extracts orders → Snowflake.dvt_raw.crm__orders (incremental)
  2. Snowflake executes model SQL → temp result
  3. Sling streams result from Snowflake → S3 as Delta Lake format
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
