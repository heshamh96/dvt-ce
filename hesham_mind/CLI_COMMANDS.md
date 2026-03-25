# DVT CLI Commands

## Overview

DVT provides a `dvt` CLI entry point alongside the stock `dbt` entry point.
Both are installed by `dvt-ce`. The `dbt` command runs stock dbt-core behavior.
The `dvt` command runs DVT-enhanced behavior (extraction, Sling, cross-target, etc.).

Users should use `dvt` for all operations. The `dbt` command remains for backward
compatibility and debugging.

## Command Reference

### dvt sync

Bootstrap the DVT environment. Reads profiles.yml and installs everything needed.

```bash
dvt sync [--profiles-dir PATH] [--python-env PATH]
```

**What it does:**
1. Reads profiles.yml (default `~/.dbt/profiles.yml` or `--profiles-dir`)
2. For each output, determines the adapter type and installs:
   - `dbt-postgres`, `dbt-snowflake`, `dbt-bigquery`, etc.
3. For bucket outputs (s3, gcs, azure), installs cloud SDKs:
   - `boto3`, `google-cloud-storage`, `azure-storage-blob`, etc.
4. Installs/verifies DuckDB and its extensions:
   - `httpfs`, `delta`, `postgres_scanner`, `mysql_scanner`, etc.
   - Extensions selected based on adapter types in profiles.yml
5. Verifies `sling` binary is installed and accessible
6. Tests all connections (databases + buckets)
7. Reports status

**Flags:**
- `--profiles-dir` — path to directory containing profiles.yml (default: `~/.dbt/`)
- `--python-env` — path to Python environment to install into (default: current env)
- `--skip-test` — skip connection testing
- `--dry-run` — show what would be installed without installing

**Example output:**
```
dvt sync
  Adapters:
    snowflake .... dbt-snowflake 1.9.0 [installed]
    postgres ..... dbt-postgres 1.9.0 [installing...]
    mysql ........ dbt-mysql 1.9.0 [installing...]
  Buckets:
    s3 ........... boto3 1.34.0 [installed]
    gcs .......... google-cloud-storage 2.14.0 [installing...]
  DuckDB:
    core ......... duckdb 1.1.0 [installed]
    httpfs ....... [installed]
    delta ........ [installed]
    postgres ..... [installing...]
  Sling:
    binary ....... sling v1.5.12 [installed]
  Connections:
    prod_snowflake .. OK (Snowflake, 0.3s)
    source_postgres . OK (PostgreSQL 16.2, 0.1s)
    data_lake ...... OK (S3, us-east-1, 0.2s)
```

---

### dvt run

Extract sources, transform models, load to targets. The core command.

```bash
dvt run [--select SELECTOR] [--exclude SELECTOR] [--target TARGET]
        [--threads N] [--full-refresh] [--vars JSON]
```

**What it does:**
1. Builds the manifest (parses models, sources, tests)
2. Injects extraction nodes into the DAG for sources with remote connections
3. Selects nodes based on `--select` / `--exclude`
4. Executes in DAG order:
   - Extraction nodes: Sling streams source → default target
   - Model nodes: pushdown via adapter OR cross-target via Sling
   - Respects `--threads` for parallel execution
5. Writes `run_results.json`

**Flags:**
- `--select` / `-s` — node selection (same syntax as dbt: `tag:daily`, `model_a+`, etc.)
- `--exclude` — exclude nodes from selection
- `--target` / `-t` — override default target for ALL models in this run
- `--threads` — number of parallel threads
- `--full-refresh` — force full-refresh for all incremental models AND extraction nodes
- `--vars` — JSON dict of project variables
- `--fail-fast` — stop on first failure
- `--defer` — defer to a previous manifest for unselected nodes

**When `--select my_model` is used:**
DVT extracts ONLY the sources that `my_model` depends on, not all sources.
The DAG traversal determines the minimal set of extraction nodes needed.

---

### dvt build

Run + test + snapshot in one command (same as `dbt build`).

```bash
dvt build [--select SELECTOR] [--exclude SELECTOR] [--target TARGET]
          [--threads N] [--full-refresh] [--vars JSON]
```

Executes extraction nodes, models, tests, snapshots, and seeds in DAG order.
Same as `dvt run` but includes all resource types.

---

### dvt compile

Compile models without executing. Shows the extraction plan.

```bash
dvt compile [--select SELECTOR] [--target TARGET] [--vars JSON]
```

**What it does:**
1. Parses and compiles all models (Jinja → SQL)
2. Resolves execution paths (pushdown vs cross-target)
3. Shows the extraction plan (which sources need Sling, what mode)
4. Writes compiled SQL to `target/compiled/`
5. Does NOT execute anything

---

### dvt test

Run dbt tests on the target.

```bash
dvt test [--select SELECTOR] [--exclude SELECTOR] [--target TARGET]
```

Standard dbt test behavior. Tests run on the target database via the adapter.
Tests can reference extraction nodes (source freshness, row counts, etc.).

---

### dvt seed

Load CSV files to the target via Sling.

```bash
dvt seed [--select SELECTOR] [--full-refresh] [--target TARGET] [--threads N]
```

**What it does:**
1. Discovers CSV files in `seed-paths` (from `dbt_project.yml`)
2. For each seed, calls Sling to stream the CSV to the target
3. Uses Sling's bulk loading (COPY, bcp, etc.) for performance
4. Default mode: if table exists, truncate + reload
5. `--full-refresh`: drop table, recreate from CSV

**`--target` flag:**
Redirects ALL seeds to the specified target (database or bucket).
```bash
dvt seed --target staging_db          # load seeds to staging database
dvt seed --target data_lake           # load seeds as files to S3
```

**Why Sling for seeds:**
dbt's native seed loader uses Python `agate` + batch INSERT statements.
A 100K row CSV takes minutes. Sling does it in seconds using native bulk loading.

---

### dvt show

Run a query locally in DuckDB without hitting the warehouse.

```bash
dvt show [--select MODEL] [--inline SQL] [--limit N] [--output FORMAT]
```

**What it does:**
1. Starts DuckDB in-process (ephemeral)
2. ATTACHes to source databases (Postgres/MySQL/SQLite) or uses scanner extensions
3. Compiles and executes the model SQL in DuckDB
4. If model SQL uses target-specific dialect, SQLGlot transpiles to DuckDB SQL
5. Displays results in the terminal

**Flags:**
- `--select` — model to show
- `--inline` — raw SQL to execute (supports Jinja: `{{ source() }}`, `{{ ref() }}`)
- `--limit` — max rows to display (default: 100)
- `--output` — output format: `table` (default), `csv`, `json`

---

### dvt debug

Check all connections and environment health.

```bash
dvt debug [--profiles-dir PATH]
```

**What it does:**
1. Verifies profiles.yml is valid
2. Tests ALL connections (databases + buckets)
3. Checks Sling binary availability and version
4. Checks DuckDB extensions
5. Checks dbt adapter installations
6. Reports per-connection: type, version, latency, status

---

### dvt retract

Drop all models from their targets in reverse DAG order.

```bash
dvt retract [--select SELECTOR] [--exclude SELECTOR] [--target TARGET]
```

**What it does:**
1. Loads manifest and selects model nodes (full dbt selector support)
2. Sorts models in reverse topological order (downstream first)
3. Connects to each model's target via native driver
4. Drops the table/view with CASCADE (for engines that support it)
5. Cleans corresponding `__model__` tables from DuckDB cache

**Flags:**
- `--select` / `-s` — node selection (same syntax as dbt: `+model`, `tag:daily`, etc.)
- `--exclude` — exclude nodes from selection
- Supports all dbt selector operators

**Examples:**
```bash
dvt retract                              # retract all models
dvt retract --select dim_customers       # retract one model
dvt retract --select +d_b_dim_officers   # retract model + all ancestors
dvt retract --exclude ephemeral_summary  # retract all except one
```

**Notes:**
- Seeds and sources are NEVER touched
- Uses CASCADE for postgres, snowflake, oracle, redshift
- Drops dependent views automatically (reverse DAG order handles this)

---

### dvt docs generate / dvt docs serve

Generate and serve documentation with cross-engine catalog and lineage.

```bash
dvt docs generate [--select SELECTOR] [--compile]
dvt docs serve [--port PORT] [--host HOST]
```

**What it does:**
1. Stamps `dvt_adapter_type` on all manifest nodes (sources + models) BEFORE generation
2. Runs dbt's GenerateTask to build the catalog
3. Enriches catalog with cross-engine column metadata from all remote sources
4. Generates HTML with DVT-branded UI:
   - DVT swirl logo + "Data Virtualization Tool" in sidebar
   - Engine-colored nodes in lineage graph (each engine has brand colors)
   - Connection badges on source and model labels
   - Target + Engine fields in model detail panels
   - Column metadata with native data types from each engine

---

### dvt deps

Install dbt packages from `packages.yml`.

```bash
dvt deps
```

Stock dbt behavior. Downloads packages to `dbt_packages/`.

---

### dvt init

Initialize a new DVT project.

```bash
dvt init [PROJECT_NAME]
```

Creates a new project with DVT-specific templates:
- `dbt_project.yml` with DVT extraction schema config
- `profiles.yml` template with multi-adapter + bucket examples
- `sources.yml` template with Sling extraction config
- `models/` with example cross-engine model

---

### dvt clean

Remove artifacts.

```bash
dvt clean
```

Removes `target/`, `dbt_packages/`, and any DVT-specific caches.

---

### dvt list (dvt ls)

List resources in the project.

```bash
dvt ls [--select SELECTOR] [--resource-type TYPE] [--output FORMAT]
```

Lists models, sources, tests, seeds, extraction nodes.
Shows execution path (pushdown/cross-target) and target for each model.

---

### dvt snapshot

Run snapshot models.

```bash
dvt snapshot [--select SELECTOR] [--target TARGET] [--threads N]
```

Standard dbt snapshot behavior with DVT extraction support.

---

### dvt source freshness

Check source freshness.

```bash
dvt source freshness [--select SELECTOR] [--output PATH]
```

Checks freshness of source tables. For remote sources, queries the source
database directly (not the cached extraction).

---

### dvt run-operation

Run a macro.

```bash
dvt run-operation MACRO_NAME [--args JSON]
```

Stock dbt behavior.
