# DVT Rules & Specifications

> **Document Version:** 6.0  
> **Last Updated:** March 2026  
> **Status:** Authoritative Reference

This document defines the complete ruleset for DVT (Data Virtualization Tool) behavior. All implementation decisions must conform to these rules.

---

## Table of Contents

1. [Core Principles](#1-core-principles)
2. [Target Rules](#2-target-rules)
3. [Source Rules](#3-source-rules)
4. [Data Movement Rules (Sling)](#4-data-movement-rules-sling)
5. [Materialization Rules](#5-materialization-rules)
6. [DAG & Execution Rules](#6-dag--execution-rules)
7. [Writing Rules](#7-writing-rules)
8. [Seed Rules](#8-seed-rules)
9. [Test Rules](#9-test-rules)
10. [Hook Rules](#10-hook-rules)
11. [Edge Cases & Validation](#11-edge-cases--validation)
12. [Backward Compatibility](#12-backward-compatibility)

---

## 1. Core Principles

### 1.1 Three Execution Paths

DVT automatically determines how to execute each model based on where its sources live relative to its target. The user writes standard dbt models — DVT handles the rest.

**Path 1 — Default Pushdown:** All sources on the default target → standard dbt. No Sling, no DuckDB. User writes in target dialect.

**Path 2 — Non-Default Pushdown:** Model targets a non-default adapter (e.g., `config(target='mysql_prod')`) but ALL its sources are on that same non-default target. Pure pushdown via the non-default adapter. User writes in that target's dialect.

**Path 3 — Extraction (cross-engine):** Model references source(s) whose `connection` != model's target. DVT detects this automatically. User writes in **DuckDB SQL** for all extraction models. Two sub-paths (DVT chooses automatically, invisible to user):

- **Sub-Path 3a — Sling Direct:** Model references exactly ONE remote `source()` or `ref()`. Sling streams directly from source → target model table. Supports incremental. No DuckDB involved.
- **Sub-Path 3b — DuckDB Compute:** Model references 2+ remote `source()` and/or `ref()`. Sling streams each source → DuckDB (in-memory). Model SQL runs in DuckDB. Sling streams result from DuckDB → target model table. Table materialization only (no incremental).

```
User writes:
  {{ source('crm', 'customers') }}    ← sources.yml says connection: source_postgres
  Model target: prod_snowflake        ← from profiles.yml default

DVT detects:
  source_postgres != prod_snowflake → Extraction path
  Single remote source → Sling Direct (sub-path 3a)

User sees:
  Standard dbt run. No extra config needed.
  Result lands directly as the model's named table on prod_snowflake.
```

**RULE 1.1.1:** The user **NEVER** configures Sling directly. No `sling:` blocks in sources.yml, no `connection` config on models, no Sling YAML files. DVT handles all Sling orchestration internally based on the source's `connection` and the model's `target`.

**RULE 1.1.2:** There are **no hidden staging tables.** No `_dvt` schema. Results land directly as the model's named table on the target.

### 1.2 Pushdown Preference

DVT **always prioritizes Adapter Pushdown** over data movement:

```
IF source.connection == model.target THEN
    USE Adapter Pushdown (standard dbt — SQL execution on target database)
ELSE IF single remote source/ref THEN
    USE Sling Direct (stream source → target model table)
ELSE (multiple remote sources/refs) THEN
    USE DuckDB Compute (Sling → DuckDB → Sling → target model table)
```

**Rationale:** Adapter pushdown is fastest. Sling Direct is efficient for single-source extraction. DuckDB handles multi-source joins that can't be pushed down.

### 1.3 Data Movement Engines

DVT uses **two** engines for data movement and cross-engine compute:

**Sling** handles all data streaming:
- Source extraction to model's target (single-source extraction: Sling Direct)
- Streaming sources into DuckDB (multi-source extraction)
- Streaming results from DuckDB to target (multi-source extraction)
- Cross-target ref resolution (when ref'd model's target != referencing model's target)
- Seed loading (CSV-to-database bulk loading)
- Cross-target model materialization (when model target != default target and target is a bucket)

**DuckDB** handles multi-source extraction compute:
- When a model references 2+ remote sources/refs, DuckDB is the in-process compute engine
- Sling streams each remote source into DuckDB, model SQL runs in DuckDB, Sling streams the result to the target
- DuckDB is also used for `dvt show` (local ad-hoc queries)

DVT does **NOT** use Spark, custom JDBC code, or PySpark.

### 1.4 User Experience is dbt

The user writes:
- `profiles.yml` — with multiple outputs (databases + buckets)
- `sources.yml` — with `connection:` property on each source
- `models/*.sql` — standard dbt SQL with `{{ source() }}`, `{{ ref() }}`, `{{ config() }}`
- `seeds/*.csv` — standard CSV seed files

**That's it.** No Sling config, no extraction models, no connection config on models, no hidden staging tables. DVT figures out the rest.

**Dialect rule for extraction models:** All models with remote sources (extraction path) must be written in **DuckDB SQL**. For single-source models, the SQL is universal enough (`SELECT * FROM ...`) that Sling executes it directly on the source. For multi-source models, DuckDB executes the SQL.

| Scenario | User Writes In | Runs On |
|---|---|---|
| All sources local (default pushdown) | Target dialect | Target DB via adapter |
| All sources on non-default target | That target's dialect | That target DB via non-default adapter |
| Single remote source (extraction) | DuckDB SQL | Source DB via Sling (SQL is universal enough) |
| Multiple remote sources (extraction) | DuckDB SQL | DuckDB (in-process) |

### 1.5 DuckDB Role

DuckDB serves two purposes in DVT:

1. **Cross-engine compute for multi-source extraction** (`dvt run`) — when a model references 2+ remote sources, Sling streams each source into DuckDB, the model SQL executes in DuckDB, and Sling streams the result to the target. This is a P1 feature.

2. **Local development** (`dvt show`) — run model queries locally without hitting the warehouse. SQLGlot transpiles target-dialect SQL to DuckDB SQL for local execution.

3. **Local file/API processing** — reading CSV/Parquet/JSON sources locally.

4. **Virtual federation (future)** — ephemeral cross-source queries via ATTACH.

---

## 2. Target Rules

### 2.1 Target Selection Hierarchy

```
Priority (Highest to Lowest):
1. CLI: --target <name>            (FORCES global override)
2. Model Config: target: <name>    (model-specific)
3. profiles.yml: default target    (base default)
```

### 2.2 profiles.yml Requirements

**RULE 2.2.1:** profiles.yml supports **multiple output types** per profile:
- Database connections (postgres, snowflake, bigquery, redshift, mysql, sqlserver, oracle, etc.)
- Cloud bucket connections (s3, gcs, azure)
- All outputs are first-class: they can be source connections OR model targets

**RULE 2.2.2:** Each output is identified by its name and used via:
- `target:` in profile config (default target for models)
- `connection:` in sources.yml (where source data lives)
- `config(target='name')` in models (per-model target override)

### 2.3 Cross-Target References (ref)

**RULE 2.3.1:** When a model refs another model with a different target:
1. The ref'd model is **executed first** (respecting DAG order)
2. The ref'd model is **materialized to its own target**
3. DVT detects the target mismatch
4. Sling streams the ref'd model's result from its target to the referencing model's target
5. The referencing model's SQL executes on its target via adapter pushdown

```
Model A (target: postgres)    → materialized on postgres
    |
    | ref
    v
Model B (target: snowflake)   → DVT: Sling moves Model A result pg → sf
                              → Model B SQL runs on snowflake (pushdown)
```

### 2.4 Global Target Override (CLI)

**RULE 2.4.1:** When `--target` is specified via CLI:
- **ALL** models are forced to materialize in the specified target
- Any source whose connection differs from this target → Sling extraction
- Model-level target configs are **overridden**

**RULE 2.4.2:** Global target override implications:
```
dvt run --target snowflake_prod

Result:
- All models materialize to snowflake_prod
- Sources with connection != snowflake_prod → Sling extracts automatically
- Sources with connection == snowflake_prod → Pushdown (no data movement)
```

### 2.5 Bucket Targets

**RULE 2.5.1:** Cloud buckets (S3, GCS, Azure) are first-class targets in profiles.yml.

**RULE 2.5.2:** When a model's target is a bucket:
- Model SQL executes on the **default database target** (where upstream data lives)
- Sling streams the result to the bucket in the configured format
- Default format: `delta`. Configurable: `parquet`, `csv`, `json`, `jsonlines`

**RULE 2.5.3:** Bucket targets support `format` and `path` model config:
```sql
{{ config(target='data_lake', format='delta', path='analytics/orders/') }}
```

### 2.6 Target Resolution Summary

| Scenario | Model Target | Execution Path |
|----------|--------------|----------------|
| No config, no CLI | profiles.yml default | Pushdown if sources match, Sling if not |
| Model config: target_x | target_x | Pushdown if sources match, Sling if not |
| CLI: --target target_y | target_y (forced) | Likely Sling for remote sources |
| Model config + CLI | CLI wins | Likely Sling for remote sources |
| Model config: s3_bucket | s3_bucket | SQL on default target, Sling to bucket |

---

## 3. Source Rules

### 3.1 Source Connection Requirements

**RULE 3.1.1:** All sources **MUST** have an explicit `connection` property in `sources.yml`.

```yaml
sources:
  - name: crm
    connection: source_postgres    # REQUIRED: output name from profiles.yml
    schema: public
    tables:
      - name: customers
      - name: orders
```

**RULE 3.1.2:** If a source does not have a `connection` property:
- DVT raises a **compilation error**: `"DVT100: Source '<name>' must have a 'connection' property specifying its database connection"`
- DVT does **NOT** assume a default

**RULE 3.1.3:** The `connection` value must match an output name in profiles.yml:
- If it does not exist: DVT raises **compilation error**: `"DVT101: Source '<name>' references connection '<conn>' which does not exist in profiles.yml"`

**RULE 3.1.4:** `sources.yml` is **metadata only**. It declares:
- What sources exist (name, tables)
- Where they live (connection)
- Their schema/database
- Standard dbt source properties (descriptions, tests, freshness, columns)

It does **NOT** contain any extraction config, Sling config, or data movement config.

**Rationale:** Explicit connections let DVT automatically determine when data movement is needed. The user declares WHERE data lives; DVT handles HOW to get it.

### 3.2 Source Resolution

**RULE 3.2.1:** When a model references `{{ source('crm', 'customers') }}`:
1. DVT looks up the source's `connection` in sources.yml → `source_postgres`
2. DVT resolves the model's target → `prod_snowflake`
3. DVT counts how many remote sources/refs the model has
4. Comparison:
   - If `source_postgres == prod_snowflake` → source is local. Resolve to `schema.table` on target. Standard dbt.
   - If `source_postgres != prod_snowflake` → source is remote. DVT triggers extraction.

**RULE 3.2.2:** For remote sources, DVT determines the extraction sub-path:
- **Single remote source/ref → Sling Direct (sub-path 3a):** Sling streams data directly from the source to the model's named table on the target. No intermediate tables.
- **Multiple remote sources/refs → DuckDB Compute (sub-path 3b):** Sling streams each remote source into DuckDB (in-memory). Model SQL runs in DuckDB. Sling streams the result to the model's named table on the target.

**RULE 3.2.3:** In both sub-paths, the result lands directly as the model's named table. There are no hidden staging tables, no `_dvt` schema, no intermediate copies on the target.

### 3.3 Extraction Model SQL Dialect

**RULE 3.3.1:** All extraction models (any model with at least one remote source) must be written in **DuckDB SQL** syntax. This is the user-facing rule.

**RULE 3.3.2:** For single-source extraction (sub-path 3a), the SQL is typically a simple `SELECT * FROM ...` or `SELECT columns FROM ...` which is universal across dialects. Sling executes the extraction on the source database.

**RULE 3.3.3:** For multi-source extraction (sub-path 3b), DuckDB executes the model SQL after all remote sources have been streamed into DuckDB.

**RULE 3.3.4:** For incremental single-source models that use `is_incremental()` with source-side WHERE clauses, DVT must format the watermark literal in the **source's dialect** (see Section 4.4).

---

## 4. Data Movement Rules

### 4.1 When Data Movement is Triggered

Data movement is triggered automatically in these scenarios:

| Scenario | Detection | Action |
|---|---|---|
| Single remote source/ref | `source.connection != model.target` (1 remote) | **Sling Direct:** stream source → model table on target |
| Multiple remote sources/refs | `source.connection != model.target` (2+ remote) | **DuckDB Compute:** Sling → DuckDB → Sling → model table on target |
| Ref'd model target != this model target | `ref_model.target != model.target` | Move ref'd result → this model's target |
| Model target is a bucket | `model.target.type in (s3, gcs, azure)` | Move result from default target → bucket |
| Seed loading | `dvt seed` command | Load CSV → target via bulk loading |

**RULE 4.1.1:** Data movement is NEVER triggered when all sources and refs are on the model's target. In that case, pure adapter pushdown is used (standard dbt behavior).

### 4.2 Sub-Path 3a: Sling Direct (Single Remote Source)

**Condition:** Model references exactly ONE remote `source()` or `ref()`.

**RULE 4.2.1:** When a model references a single source on a different connection:
1. DVT generates a Sling extraction task for that source table
2. Sling executes `SELECT * FROM schema.table` on the source database
3. Sling streams the result **directly** to the model's named table on the target
4. No intermediate tables, no staging schema, no hidden copies

**RULE 4.2.2:** For `table` materialized models:
- Sling mode: `full-refresh` (create/replace the model's table on the target)

**RULE 4.2.3:** For `incremental` materialized models:
- See Section 4.4 (Cross-Engine Incremental)
- Sling handles watermark filtering + merge directly on the model's table

**RULE 4.2.4:** Sling Direct is efficient for large tables — streaming, low memory footprint.

### 4.3 Sub-Path 3b: DuckDB Compute (Multiple Remote Sources)

**Condition:** Model references 2+ remote `source()` and/or `ref()`.

**RULE 4.3.1:** When a model references multiple remote sources:
1. For each remote source/ref, Sling streams data into DuckDB (in-memory)
2. Model SQL executes in DuckDB (user wrote it in DuckDB SQL)
3. Sling streams the result from DuckDB to the model's named table on the target
4. No intermediate tables on the target, no staging schema

**RULE 4.3.2:** DuckDB Compute only supports `table` materialization:
- `incremental` → DVT raises **compilation error** `"DVT112: Incremental materialization is not supported for models with multiple remote sources. Use table materialization instead."`
- `view` → coerced to `table` (DVT001 warning)
- `ephemeral` → not supported (DVT110 error)

**RULE 4.3.3:** DuckDB is ephemeral — it starts in-process, loads sources, runs the model SQL, and shuts down. No persistent state.

**RULE 4.3.4:** The user writes DuckDB SQL for the model. DVT handles all Sling orchestration to load sources into DuckDB and stream the result to the target.

### 4.4 Cross-Engine Incremental Models

**RULE 4.4.1:** Incremental materialization is **only supported for single-source extraction** (sub-path 3a — Sling Direct). Multi-source extraction (sub-path 3b — DuckDB) cannot be incremental (DVT112 error).

**RULE 4.4.2:** When a single-source `incremental` model references a remote source:
1. DVT pre-resolves the watermark value from the **target** model table
2. DVT formats the watermark as a **dialect-specific literal** for the source engine
3. DVT generates an extraction query with the watermark filter
4. Sling executes the filtered query on the source (only delta rows)
5. Sling merges the delta directly into the model's table on the target
6. No staging tables involved — Sling operates directly on the model's named table

**RULE 4.4.3:** dbt incremental strategy → Sling merge strategy mapping:

| dbt Strategy | Sling Behavior |
|---|---|
| `append` | Sling `incremental` mode, no primary_key (append-only) |
| `merge` | Sling `incremental` mode with primary_key (upsert) |
| `delete+insert` | Sling `incremental` mode with `merge_strategy: delete_insert` |
| `insert_overwrite` | Sling partition overwrite via target_options |

**RULE 4.4.4:** The model's `unique_key` maps to Sling's `primary_key` for the merge on the model's target table.

**RULE 4.4.5:** Watermark resolution flow:
```
1. Model SQL has: WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
2. DVT queries target: SELECT MAX(updated_at) FROM target_schema.model_table
3. Gets raw value: datetime(2024, 3, 14, 12, 0, 0)
4. Formats for source dialect (see Section 4.5)
5. Generates extraction query:
   SELECT * FROM source_schema.orders
   WHERE updated_at > TO_TIMESTAMP('2024-03-14 12:00:00.000000', 'YYYY-MM-DD HH24:MI:SS.FF6')
6. Sling executes on source, streams delta directly to model table, merges
```

**RULE 4.4.6:** On first run (target table doesn't exist): `is_incremental()` returns false. Full extraction, no watermark. Standard dbt behavior.

**RULE 4.4.7:** On `--full-refresh`: `is_incremental()` returns false. Full extraction, full model rebuild. Standard dbt behavior.

### 4.5 Dialect-Specific Watermark Formatting

**RULE 4.5.1:** DVT maintains a dialect-specific literal formatter. The formatter produces valid SQL literals for the **source** database's dialect.

**RULE 4.5.2:** Timestamp literal formats:

| Source Engine | Format |
|---|---|
| PostgreSQL | `'{value}'::TIMESTAMP` |
| MySQL / MariaDB | `'{value}'` |
| SQL Server | `CONVERT(DATETIME2, '{value}', 121)` |
| Oracle | `TO_TIMESTAMP('{value}', 'YYYY-MM-DD HH24:MI:SS.FF6')` |
| Snowflake | `TO_TIMESTAMP('{value}')` |
| BigQuery | `TIMESTAMP '{value}'` |
| Redshift | `'{value}'::TIMESTAMP` |
| Databricks | `TIMESTAMP '{value}'` |
| ClickHouse | `toDateTime64('{value}', 6)` |
| Trino | `TIMESTAMP '{value}'` |
| DuckDB | `TIMESTAMP '{value}'` |
| SQLite | `'{value}'` |

**RULE 4.5.3:** Date literal formats follow the same pattern (e.g., Oracle: `TO_DATE(...)`, SQL Server: `CONVERT(DATE, ...)`, etc.).

**RULE 4.5.4:** Integer/numeric watermarks: plain literals (`12345`, `99.99`).

**RULE 4.5.5:** String watermarks: `'value'` (SQL Server Unicode: `N'value'`). Single quotes escaped as `''`.

**RULE 4.5.6:** If formatting fails: DVT raises **runtime error** `"DVT109: Cannot format watermark value for source dialect"`.

### 4.6 Sling Type Mapping

**RULE 4.6.1:** Sling maps types in two steps: source native → Sling general → target native.

**RULE 4.6.2:** Standard analytics types (integers, floats, strings, booleans, dates, timestamps, decimals) map cleanly across all engines. No user intervention needed.

**RULE 4.6.3:** Exotic types without cross-engine equivalents (`GEOGRAPHY`, `HSTORE`, `TSVECTOR`, `INTERVAL`, nested ARRAY/STRUCT) are mapped to `text` or `json`. DVT emits warning: `"DVT002: Column '<col>' type '<type>' has no direct equivalent on target. Mapped to text/json."`

---

## 5. Materialization Rules

### 5.1 Defaults & Validation

**RULE 5.1.1:** If materialization is unspecified:
- Use project default from `dbt_project.yml`
- If no project default, use `view`

**RULE 5.1.2:** If materialization is invalid/unknown:
- DVT raises a **compilation error**: `"DVT108: Unknown materialization '<name>'"`

### 5.2 Same-Target Execution (Adapter Pushdown)

**Condition:** All sources and refs are on the model's target (no data movement needed)

| Materialization | Behavior |
|-----------------|----------|
| `table` | Create/replace table via adapter SQL |
| `view` | Create/replace view via adapter SQL |
| `incremental` | Apply incremental strategy via adapter SQL |
| `ephemeral` | Compile as CTE, inject into downstream queries |

This is **stock dbt behavior**. DVT delegates entirely to the dbt adapter.

### 5.3 Extraction Execution (Cross-Engine)

**Condition:** At least one source or ref is on a different connection/target

**Sub-Path 3a: Sling Direct (single remote source/ref):**

| Materialization | Behavior |
|-----------------|----------|
| `table` | Sling streams source → model's named table on target |
| `incremental` | Sling streams delta → merges into model's table on target (watermark-filtered) |
| `view` | **Coerced to table** (DVT001 warning) |
| `ephemeral` | **Not supported** (DVT110 error) |

**Sub-Path 3b: DuckDB Compute (multiple remote sources/refs):**

| Materialization | Behavior |
|-----------------|----------|
| `table` | Sling → DuckDB (each source). DuckDB runs model SQL. Sling → model's table on target |
| `incremental` | **Not supported** (DVT112 error) |
| `view` | **Coerced to table** (DVT001 warning) |
| `ephemeral` | **Not supported** (DVT110 error) |

**Key points:**
- No hidden staging tables. Results land directly as the model's named table.
- For Sling Direct: Sling is the data movement engine, source DB is the compute engine.
- For DuckDB Compute: Sling is the data movement engine, DuckDB is the compute engine.

### 5.4 View on Extraction Models

**RULE 5.4.1:** Views are NOT supported for extraction models (models with remote sources). Views are coerced to tables:
- DVT emits warning: `"DVT001: Model '<name>' has remote sources and is materialized as view. Coercing to table."`

**RULE 5.4.2:** When a model's target is a **bucket**, views are also coerced to tables:
- DVT emits warning: `"DVT001: Model '<name>' targets a bucket but is materialized as view. Coercing to table."`

### 5.5 Ephemeral Models

**RULE 5.5.1:** Ephemeral models with all local sources are compiled as CTEs and injected into downstream queries. Standard dbt behavior.

**RULE 5.5.2:** Ephemeral models with remote sources are NOT supported:
- DVT raises **compilation error**: `"DVT110: Ephemeral materialization is not supported for models with remote sources. Use table materialization instead."`

**RULE 5.5.3:** If an ephemeral model (all local sources) is referenced by **multiple** downstream models:
- DVT emits a warning: `"DVT006: Ephemeral model '<name>' is referenced by multiple downstream models. Consider materializing it for performance."`

### 5.6 Snapshot Materialization

**RULE 5.6.1:** Snapshots work with remote sources. DVT extracts the source to the target staging area, then the snapshot runs via the adapter on the target database.

**RULE 5.6.2:** Snapshot SCD Type 2 logic executes entirely on the target adapter.

### 5.7 Bucket Materialization

**RULE 5.7.1:** When `config(target='bucket_name')`:
- Model SQL executes on the **default database target** via adapter pushdown
- Result is written to a temp table on the default target
- Sling streams the result → bucket in the configured format
- Temp table is dropped

**RULE 5.7.2:** Default bucket format: `delta`. Configurable via `config(format='...')`:
- `delta`, `parquet`, `csv`, `json`, `jsonlines`

**RULE 5.7.3:** Bucket path configurable via `config(path='...')`.

### 5.8 Materialization Summary

| Materialization | All Local | Single Remote Source (Sling Direct) | Multiple Remote Sources (DuckDB) | Bucket Target |
|-----------------|-----------|-------------------------------------|----------------------------------|---------------|
| table | Adapter SQL | Sling → model table | Sling → DuckDB → Sling → model table | Adapter SQL → Sling to bucket |
| view | Adapter SQL | **Coerced to table** (DVT001) | **Coerced to table** (DVT001) | **Coerced to table** (DVT001) |
| incremental | Adapter SQL | Sling incremental → model table | **Not supported** (DVT112) | Adapter SQL → Sling to bucket |
| ephemeral | CTE injection | **Not supported** (DVT110) | **Not supported** (DVT110) | CTE (no bucket write) |
| snapshot | Adapter SQL | Sling → model table, then snapshot | Not supported | Not applicable |

---

## 6. DAG & Execution Rules

### 6.1 Resolution Phase

```
Step 1: Parse all models, sources, seeds, tests
Step 2: Resolve TARGET for every model
        - CLI --target > model config target > profiles.yml default
Step 3: Resolve DATA MOVEMENT for every model
        - For each source(): compare source.connection vs model.target
        - For each ref(): compare ref'd model.target vs this model.target
        - If any mismatch → mark source/ref for Sling extraction
Step 4: Validate
        - Check all source connections exist in profiles.yml
        - Check all model targets exist in profiles.yml
        - Check for circular dependencies
```

### 6.2 Execution Phase

**RULE 6.2.1:** For each model in DAG order:
1. **Extract** any remote sources/refs to the model's target (Sling, parallel where possible)
2. **Execute** model SQL on the target (adapter pushdown)
3. **Load** result to alternate target if needed (Sling, for bucket targets)

```
Example DAG:

sources.yml: crm (connection: pg), erp (connection: mssql)
Model target: snowflake (default)

stg_customers (single remote source: crm.customers):
  → Sling Direct: pg.customers → sf.stg_customers (model table directly)

stg_orders (single remote source: crm.orders, incremental):
  → Sling Direct: pg.orders → sf.stg_orders (with watermark filter, merge)

stg_invoices (single remote source: erp.invoices):
  → Sling Direct: mssql.invoices → sf.stg_invoices (model table directly)

(above three can run in parallel)

combined_report (multiple remote sources: crm.customers + erp.products):
  → DuckDB Compute: Sling streams pg.customers + mssql.products → DuckDB
  → DuckDB runs model SQL (join)
  → Sling streams result → sf.combined_report

dim_customers (all local — refs stg_customers, stg_orders):
  → Pushdown: Snowflake runs SQL (standard dbt)

Tests run on snowflake.
```

**RULE 6.2.2:** Extractions for the same model **MAY** run in parallel.

**RULE 6.2.3:** Independent models **MAY** run in parallel (standard dbt, `--threads`).

### 6.3 Parallel Execution

**RULE 6.3.1:** Controlled by `--threads` (like dbt). Default: 4.

### 6.4 Error Handling

**RULE 6.4.1:** If a Sling extraction fails:
- Mark the extraction as **Error**
- Mark the dependent model as **Error**
- Mark all downstream models as **Skipped**
- Continue executing independent models

**RULE 6.4.2:** If a model fails (adapter error):
- Mark as **Error**, skip downstream, continue independent. Standard dbt.

**RULE 6.4.3:** `--fail-fast`: stop on first failure.

**RULE 6.4.4:** For incremental extraction failures: watermark not updated, next run retries safely.

---

## 7. Writing Rules

### 7.1 Table Materialization Write Behavior

**RULE 7.1.1:** Default (no `--full-refresh`): Truncate + Insert. Preserves structure, grants.

**RULE 7.1.2:** With `--full-refresh`: Drop + Create + Insert.

**RULE 7.1.3:** Applies to both pushdown and extraction-based paths.

### 7.2 Schema Evolution

**RULE 7.2.1:** Column type changes without `--full-refresh`:
- DVT raises **error** `"DVT105: Schema change detected. Use --full-refresh."`

**RULE 7.2.2:** With `--full-refresh`: table dropped and recreated. No error.

### 7.3 Incremental Write Behavior

**RULE 7.3.1:** For pushdown models: adapter handles incremental strategy (standard dbt).

**RULE 7.3.2:** For cross-engine models: Sling handles extraction merge, adapter handles model merge.

**RULE 7.3.3:** `--full-refresh`: full extraction + full model rebuild.

---

## 8. Seed Rules

### 8.1 Sling-Based Loading

**RULE 8.1.1:** All seeds use **Sling** for CSV loading. 10-100x faster than dbt's Python INSERT batching.

**RULE 8.1.2:** Sling uses native bulk loading (COPY, bcp, sqlldr) per target database.

### 8.2 Seed Write Behavior

**RULE 8.2.1:** Default (no `--full-refresh`): Truncate + Insert.

**RULE 8.2.2:** `dvt seed --full-refresh`: Drop + Create + Load.

### 8.3 Cross-Target Seeding

**RULE 8.3.1:** `dvt seed --target <name>` redirects all seeds to the specified target.

**RULE 8.3.2:** Target resolution: CLI --target > profiles.yml default.

### 8.4 Type Handling

**RULE 8.4.1:** Sling infers types from CSV. Standard dbt `column_types` overrides work.

**RULE 8.4.2:** Unknown types default to STRING/VARCHAR.

---

## 9. Test Rules

### 9.1 Test Execution Path

**RULE 9.1.1:** Tests **always** use the target adapter. Never Sling or DuckDB.

**RULE 9.1.2:** Rationale: models are already materialized on the target. Tests verify target data.

### 9.2 Cross-Target Test Limitations

**RULE 9.2.1:** Tests referencing models/sources in different targets:
- DVT raises **compilation error** `"DVT103: Test references nodes in different targets."`

---

## 10. Hook Rules

### 10.1 Hook Execution

**RULE 10.1.1:** `pre-hook`: runs on model's target adapter, BEFORE model SQL (AFTER any Sling extractions).

**RULE 10.1.2:** `post-hook`: runs on model's target adapter, AFTER model SQL (BEFORE any Sling load to bucket).

### 10.2 Pushdown Path

Standard dbt behavior. Both hooks on same adapter.

### 10.3 Limitations

**RULE 10.3.1:** Hooks are SQL on the target adapter. They do NOT run on source connections.

---

## 11. Edge Cases & Validation

### 11.1 No Upstream Dependencies
Models with no sources/refs → pushdown on target. No Sling.

### 11.2 Duplicate Source Definitions
Same source with different `connection` values → **compilation error** `"DVT102"`.

### 11.3 Self-Referencing Models
Allowed only for `incremental`. Otherwise → **compilation error** `"DVT104"`.

### 11.4 Empty Results
Zero rows → table/view still created. Not an error.

### 11.5 NULL Handling
Standard SQL semantics. NULLs preserved across Sling extraction.

### 11.6 Sling Binary Not Available
If extraction needed but Sling missing → **runtime error** `"DVT106"`.
If no extraction needed → DVT works without Sling.

---

## 12. Backward Compatibility

### 12.1 Single-Adapter Projects
**Identical to dbt.** No Sling, no DuckDB, no data movement. All pushdown.

### 12.2 Migration from dbt
1. `pip install dvt-ce`
2. Add `connection:` to sources in `sources.yml`
3. `dvt sync` to install adapters
4. `dvt run` — just works

### 12.3 Compatibility Matrix

| dbt Feature | DVT Single-Adapter | DVT Multi-Adapter |
|-------------|-------------------|-------------------|
| Models | Identical | Transparent Sling extraction + pushdown |
| Seeds | Sling-based (faster) | Sling-based + cross-target |
| Tests | Identical | Target adapter only |
| Snapshots | Identical | Sling extract + adapter snapshot |
| Hooks | Identical | Target adapter (pre/post) |
| Macros | Identical | Identical |
| Packages | Identical | Identical |
| Incremental | Identical | Single-source: Sling Direct (dialect-aware watermarks). Multi-source: not supported. |

---

## Appendix A: Warning & Error Codes

### Warnings

| Code | Message | Trigger |
|------|---------|---------|
| DVT001 | Materialization coerced | View → Table on bucket target |
| DVT002 | Type precision loss | Exotic type mapped to text/json during extraction |
| DVT003 | Extraction slow | Large table full-refresh, suggest incremental |
| DVT006 | Ephemeral multi-ref | Ephemeral referenced by multiple downstream models |

### Errors

| Code | Message | Trigger |
|------|---------|---------|
| DVT100 | Connection required | Source missing `connection` property |
| DVT101 | Connection not found | Source connection not in profiles.yml |
| DVT102 | Conflicting connections | Same source, different connections |
| DVT103 | Cross-target test | Test references multiple targets |
| DVT104 | Self-reference | Non-incremental self-referencing model |
| DVT105 | Schema change | Column types changed without --full-refresh |
| DVT106 | Sling not found | Sling binary missing when extraction needed |
| DVT108 | Unknown materialization | Invalid materialization type |
| DVT109 | Watermark format error | Cannot format watermark for source dialect |
| DVT110 | Ephemeral with remote sources | Ephemeral materialization not supported for models with remote sources |
| DVT112 | Incremental multi-source | Incremental materialization not supported for models with multiple remote sources |

---

## Appendix B: Configuration Reference

### profiles.yml

```yaml
my_project:
  target: prod_snowflake
  outputs:
    prod_snowflake:
      type: snowflake
      account: xyz123
      user: dvt_user
      password: "{{ env_var('SF_PASS') }}"
      database: ANALYTICS
      warehouse: COMPUTE_WH
      schema: PUBLIC

    source_postgres:
      type: postgres
      host: pg.internal
      port: 5432
      user: readonly
      password: "{{ env_var('PG_PASS') }}"
      dbname: production

    source_sqlserver:
      type: sqlserver
      host: mssql.internal
      port: 1433
      user: reader
      password: "{{ env_var('MSSQL_PASS') }}"
      database: ERP

    data_lake:
      type: s3
      bucket: company-data-lake
      region: us-east-1
      access_key_id: "{{ env_var('AWS_KEY') }}"
      secret_access_key: "{{ env_var('AWS_SECRET') }}"
```

### sources.yml (metadata only)

```yaml
version: 2
sources:
  - name: crm
    connection: source_postgres
    schema: public
    tables:
      - name: customers
      - name: orders

  - name: erp
    connection: source_sqlserver
    schema: dbo
    tables:
      - name: invoices
      - name: products
```

### Models

```sql
-- models/staging/stg_customers.sql
-- EXTRACTION MODEL: Single remote source → Sling Direct (sub-path 3a)
-- Written in DuckDB SQL (universal enough for Sling to execute on source)
-- DVT detects: source crm is on source_postgres, model target is prod_snowflake
-- Result lands directly as stg_customers table on prod_snowflake
{{ config(materialized='table') }}
SELECT * FROM {{ source('crm', 'customers') }}
```

```sql
-- models/staging/stg_orders.sql
-- EXTRACTION MODEL: Single remote source, incremental → Sling Direct (sub-path 3a)
-- Written in DuckDB SQL. DVT handles watermark formatting for source dialect.
-- Sling merges delta directly into stg_orders table on target.
{{ config(materialized='incremental', unique_key='id') }}
SELECT * FROM {{ source('crm', 'orders') }}
{% if is_incremental() %}
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}
```

```sql
-- models/staging/stg_combined.sql
-- EXTRACTION MODEL: Multiple remote sources → DuckDB Compute (sub-path 3b)
-- Written in DuckDB SQL. DuckDB executes the join.
-- Sling streams each source into DuckDB, runs SQL, streams result to target.
-- NOTE: Must be 'table' materialization (incremental not supported for multi-source)
{{ config(materialized='table') }}
SELECT c.id, c.name, i.invoice_total
FROM {{ source('crm', 'customers') }} c
JOIN {{ source('erp', 'invoices') }} i ON c.id = i.customer_id
```

```sql
-- models/marts/dim_customers.sql
-- PUSHDOWN MODEL: All refs are on target. Standard dbt. Written in target dialect.
{{ config(materialized='table') }}
SELECT c.*, COUNT(o.id) as order_count
FROM {{ ref('stg_customers') }} c
LEFT JOIN {{ ref('stg_orders') }} o ON c.id = o.customer_id
GROUP BY ALL
```

```sql
-- models/archive/archive_orders.sql
-- Cross-target: SQL runs on snowflake, result goes to S3 as Delta.
{{ config(materialized='table', target='data_lake', format='delta') }}
SELECT * FROM {{ ref('stg_orders') }}
```

### dbt_project.yml

```yaml
name: my_project
version: '0.1.0'
profile: 'my_project'

model-paths: ["models"]
seed-paths: ["seeds"]
test-paths: ["tests"]

# No dvt.staging_schema needed — DVT does not create hidden staging tables.
# Results land directly as model tables.
```

---

## Appendix C: Decision Summary

| Topic | Decision |
|-------|----------|
| Data movement trigger | Automatic. source.connection != model.target → extraction path. |
| Single remote source | **Sling Direct** — stream source → model table. Supports incremental. |
| Multiple remote sources | **DuckDB Compute** — Sling → DuckDB → Sling → model table. Table materialization only. |
| User Sling configuration | **NONE.** User never configures Sling. DVT handles it. |
| Model connection config | **NONE.** No `connection` on models. Only on sources. |
| Extraction config on sources | **NONE.** No `sling:` blocks. sources.yml is metadata only. |
| Hidden staging tables | **NONE.** No `_dvt` schema. Results land directly as model tables. |
| Extraction model SQL dialect | **DuckDB SQL.** All models with remote sources are written in DuckDB SQL. |
| Cross-engine incremental | Only for single-source (Sling Direct). DVT pre-resolves watermark, Sling extracts delta. |
| Multi-source incremental | **NOT supported.** DVT112 error. Use table materialization. |
| Watermark formatting | Dialect-specific literals (Oracle: TO_TIMESTAMP, SQL Server: CONVERT, etc.) |
| Type mapping | Sling general type system. DVT002 warning for exotic types. |
| Cross-target refs | DVT detects, Sling moves result. Transparent to user. |
| Ephemeral with remote sources | **NOT supported.** DVT110 error. Use table materialization. |
| Snapshots with remote sources | Supported via Sling Direct (single source). |
| Failed extraction | Error model + skip downstream. Incremental safe (watermark not committed). |
| Parallel execution | --threads. Extractions for same model can parallelize. |
| Table write mode | Truncate+Insert default. Drop+Create on --full-refresh. |
| Seed loading | Sling bulk load. --full-refresh drops. --target redirects. |
| DuckDB | dvt show + multi-source extraction compute + local files + virtual federation (future). |
| Bucket targets | First-class. Delta default. Configurable format. |
| Sling not installed | Error only if extraction needed. Optional otherwise. |

---

*End of DVT Rules Document*
