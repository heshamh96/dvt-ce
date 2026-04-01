# DVT CLI Commands

## Overview

DVT provides a `dvt` CLI entry point alongside the stock `dbt` entry point.
Both are installed by `dvt-ce`. The `dbt` command runs stock dbt-core behavior.
The `dvt` command runs DVT-enhanced behavior (extraction, Sling, cross-target, etc.).

Users should use `dvt` for all operations. The `dbt` command remains for backward
compatibility and debugging.

## Command Reference

### dvt sync

Self-healing environment bootstrap. Reads profiles.yml and ensures everything works.

```bash
dvt sync [--profiles-dir PATH] [--skip-test] [--dry-run]
```

**What it does (in order):**
1. Purges dbt-core if present (conflicts with dvt-ce namespace)
2. Reads profiles.yml (default `~/.dvt/` then `~/.dbt/`)
3. **Core deps verification**: checks duckdb, pyarrow, sqlglot, sling are importable — installs if missing (catches partial installs from memory/network failures)
4. **Adapter driver verification**: checks the actual database driver (psycopg2, mysql.connector, pyodbc, oracledb, snowflake.connector, etc.) — not just the adapter module. Installs missing drivers via `uv pip` or `pip`.
5. Installs cloud SDKs for bucket connections (boto3, google-cloud-storage, etc.)
6. Installs DuckDB extensions (delta, json, postgres_scanner, mysql_scanner)
7. Verifies Sling binary (bootstraps if needed)

**Flags:**
- `--profiles-dir` — path to directory containing profiles.yml (default: `~/.dvt/`)
- `--skip-test` — skip connection testing
- `--dry-run` — show what would be installed without installing

**Example output:**
```
dvt sync — reading profiles from: /home/hex/.dvt

  Found 9 outputs across 1 profile(s)
  Adapter types: databricks, mysql, oracle, postgres, snowflake, sqlserver

  Adapters:
    postgres ...................... 🟩
    snowflake ..................... 🟩
    mysql ......................... 🟩

  DuckDB:
    core ........................ duckdb 1.5.1 [installed]
    delta ......................... 🟩
    json .......................... 🟩
    postgres_scanner .............. 🟩

  Sling:
    binary ........................ sling 1.5.13 [installed]

  Sync complete. Environment ready.
```

**Self-healing**: dvt sync is designed to be run repeatedly. It detects and fixes:
- Missing database drivers (psycopg2 not installed → installs it)
- Missing core deps (duckdb failed to install due to memory → retries)
- dbt-core conflicts (accidentally installed → removes it)
- New adapters added to profiles.yml → installs their drivers

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

### dvt init

Initialize a new DVT project.

```bash
dvt init [PROJECT_NAME] [--skip-profile-setup]
```

**What it does:**
1. If no project name given, defaults to current directory name and scaffolds **in-place** (no subdirectory)
2. If project name given, creates `PROJECT_NAME/` subdirectory with scaffold
3. Creates `~/.dvt/` directory and `profiles.yml` if they don't exist
4. Prompts user to select one of 13 database adapters
5. Writes a template profile to `~/.dvt/profiles.yml` (appends if file exists)
6. `--skip-profile-setup` still creates a default postgres profile
7. Does NOT load adapter drivers (defers to `dvt sync` — no psycopg2 crash)
8. Output references DVT GitHub + Discord, not dbt

**Notes:**
- Replaces dbt's InitTask entirely (DvtInitTask)
- Creates: `dbt_project.yml`, `models/`, `seeds/`, `tests/`, `macros/`, `snapshots/`, `analyses/`
- Sample model: `models/example/my_first_model.sql`

---

### dvt debug

Check all connections and environment health.

```bash
dvt debug [--profiles-dir PATH] [--target TARGET]
```

**What it does:**
1. Tests each connection with BOTH the native adapter driver AND Sling
2. Shows dual status: `Adapter: OK | Sling: OK`
3. Connection is OK if adapter passes; Sling failure is visible but non-blocking
4. Checks Sling binary availability and version
5. Checks DuckDB availability and version
6. Reports per-connection: type, latency, status
7. Sling sslmode fallback: retries with `sslmode=disable` if `prefer` fails

**Output format:**
```
  Connections:
    🟩 pg_dev ........................ postgres (1.6s)
       Adapter: OK | Sling: OK
    🟩 sf_dev ........................ snowflake (5.8s)
       Adapter: OK | Sling: OK
    🟩 mssql_docker .................. sqlserver (1.6s)
       Adapter: OK | Sling: FAIL
```

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
   - DVT swirl logo + "Data Catalog" in sidebar
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
