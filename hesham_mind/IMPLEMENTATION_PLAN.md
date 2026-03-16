# DVT Implementation Plan — Shared Memory

> **Last Updated:** March 2026  
> **Status:** Living document — updated after each phase

This file tracks everything that has been done and what is upcoming.
It acts as shared memory between sessions.

---

## Foundation

- **dvt-ce**: dbt-core 1.9.10 with package renamed to `dvt-ce`. Provides `dbt.*` + `dvt.*` namespaces.
- **dvt-adapters**: All 13 engine adapters in one package. Provides `dbt.adapters.*` + `dbt.include.*`.
- **Branch:** `new_dev` on dvt-ce repo, `dev` on dvt-adapters repo.
- **Test project:** `Testing_Playground/trial_19_dvt_ce_pypi/Coke_DB` with 5 Docker databases.
- **Both packages installed as local editable** in trial_19 `.venv`.

---

## COMPLETED

### Phase 0: Scaffold + Sync [DONE]

| Item | Status | Details |
|------|--------|---------|
| P0.1: DVT CLI scaffold | DONE | `dvt` entry point with all dbt commands + `dvt sync`. Resilient `dvt.cli.__init__` falls back to sync-only CLI when dbt imports fail. |
| P0.2: `dvt sync` | DONE | Self-healing: purges dbt-core conflicts, installs database drivers (not dbt-* packages), installs DuckDB extensions, verifies Sling. Favors `uv` over `pip`. |

### Phase 1: Core Extraction Pipeline [DONE]

| Item | Status | Details |
|------|--------|---------|
| P1.1: Connection mapper | DONE | 16 adapter types → Sling URLs. Fixed: postgres sslmode, sqlserver TLS, oracle service_name, databricks token/port/http_path format. |
| P1.2: Watermark formatter | DONE | 13 dialects × 4 types (timestamp, date, integer, string). Oracle TO_TIMESTAMP, SQL Server CONVERT, BigQuery TIMESTAMP prefix, etc. |
| P1.3: Source connection parsing | DONE | Reads `connection:` directly from sources.yml (parallel to dbt parsing). Also added `connection` field to dbt's `UnparsedSourceDefinition`. |
| P1.4: Target resolver | DONE | `resolve_all_models()` classifies each model into execution paths. Reads source connections, compares vs model target. |
| P1.5: Sling client | DONE | `SlingClient` with lazy import. Methods: `extract_to_target`, `extract_to_duckdb`, `load_from_duckdb`, `load_seed`, `load_cross_target`. |
| P1.6: DvtRunTask + DvtModelRunner | DONE | `DvtRunTask(RunTask)` resolves models, returns `DvtModelRunner` for extraction. Source ref rewriting (longest-first replacement). |
| P1.7: DvtBuildTask | DONE | `DvtBuildTask(DvtRunTask, BuildTask)` — inherits extraction paths for `dvt build`. |

### Phase 2: Seeds, Debug, Show [DONE]

| Item | Status | Details |
|------|--------|---------|
| P2.1: `dvt seed` via Sling | DONE | `DvtSeedTask` + `DvtSeedRunner`. Sling bulk loads CSV → target (COPY, bcp). 10-100x faster than dbt's agate INSERT. |
| P2.2: `dvt show` via DuckDB | DONE | `DvtShowTask`. DuckDB in-memory, ATTACHes to Postgres/MySQL, runs inline SQL. Cross-engine JOINs locally. |
| P2.3: `dvt debug` | DONE | `DvtDebugTask`. Tests all connections (or `--target X`) via Sling replication ping. |
| P2.4: Broad testing | DONE | 7/7 federation models, 27/27 build nodes, 9/9 connections. |

### Phase 3: Package Consolidation [DONE]

| Item | Status | Details |
|------|--------|---------|
| P3.1: dvt-adapters rebuild | DONE | 13 engines from upstream + community repos. Import fixes for mysql, databricks. |
| P3.2: dvt-ce depends on dvt-adapters | DONE | Clean stack: zero dbt-core, zero dbt-adapters from PyPI. |
| P3.3: Sync installs drivers only | DONE | No `dbt-*` packages. Only driver deps (psycopg2, oracledb, etc.). |

### Phase 4: Persistent DuckDB Cache + Incremental [DONE]

| Item | Status | Details |
|------|--------|---------|
| P4.1: `.dvt/` cache directory | DONE | Created at `.dvt/cache.duckdb` in project dir. Managed by DvtCache. |
| P4.2: Persistent DvtCache engine | DONE | `DvtCache` replaces ephemeral `DuckDBCompute`. Thread-safe file lock for parallel execution. `ensure_created()`, `close_and_release()`, `reopen()`, `destroy()`. |
| P4.3: Unified extraction path | DONE | Killed Sling Direct sub-path. ALL extraction: Sling→DuckDB cache→Sling→target. Two-way dispatch (pushdown vs extraction). |
| P4.4: Cache-per-source | DONE | Sources cached as `{source}__{table}`. Multiple models share same cached source. |
| P4.5: Incremental extraction | DONE | `is_incremental()` checks DuckDB cache. Watermark from TARGET. Sling extracts delta. `{{ this }}` rewritten to cache model table. |
| P4.6: `--full-refresh` | DONE | Destroys `.dvt/cache.duckdb`, re-extracts everything. |
| P4.7: View/ephemeral coercion | DONE | DVT001 warning, coerced to table. |
| P4.8: E2E incremental test | DONE | Run 1: full (5 rows). Run 2: incremental (0 delta). Run 3: --full-refresh (5 rows re-extracted). |

### E2E Test Results (Trial 19 — Latest)

| Test | Result |
|------|--------|
| `dvt sync` | 6/6 adapters, DuckDB, Sling |
| `dvt debug` | 9/9 connections OK |
| `dvt seed` | PASS (Sling bulk load) |
| `dvt run pushdown_pg` | PASS (adapter pushdown) |
| `dvt run mysql_to_pg` | PASS (DuckDB cache) |
| `dvt run mariadb_to_pg` | PASS (DuckDB cache) |
| `dvt run mssql_to_pg` | PASS (DuckDB cache) |
| `dvt run oracle_to_pg` | PASS (DuckDB cache) |
| `dvt run cross_pg_mysql` | PASS (DuckDB cache, JOIN) |
| `dvt run cross_all_docker_to_pg` | PASS (DuckDB cache, 5-engine JOIN) |
| `dvt run incremental_mysql_to_pg` | PASS (incremental via cache) |
| `dvt build` (27 nodes) | 27/27 PASS |
| `dvt show` cross-engine | PASS (DuckDB ATTACH) |
| `.dvt/cache.duckdb` persists | 5 source tables + 6 model results |

### Known Issues

| Issue | Details | Workaround |
|-------|---------|------------|
| Incremental + parallel | Incremental models may see stale `is_incremental()` when run alongside other models due to dbt's partial_parse cache | Run incremental models in isolation or clear target/partial_parse |

---

## UPCOMING

### Phase 5: Federation Optimizer [NEXT — HIGH PRIORITY]

Reduce data movement — critical for single-machine operation.

| Item | Priority | Details |
|------|----------|---------|
| P5.1: Column pruning | HIGH | Analyze model SQL (SQLGlot AST) to determine which columns are actually used from each source. Generate `SELECT col1, col2` instead of `SELECT *` for extraction. |
| P5.2: Predicate pushdown | HIGH | Identify WHERE predicates that apply to a single source and push them to the extraction query. Reduces rows transferred. |
| P5.3: LIMIT pushdown | MEDIUM | If model has LIMIT, propagate to extraction query. |
| P5.4: Source-side aggregation | LOW | If model only uses aggregated data from a source, push GROUP BY to source. |

### Phase 6: Non-Default Pushdown + Polish

| Item | Priority | Details |
|------|----------|---------|
| P6.1: Non-default pushdown | HIGH | Model targets non-default adapter, all sources on that adapter → compile and execute using non-default adapter. |
| P6.2: `~/.dvt` profiles directory | MEDIUM | Add `~/.dvt` as fallback alongside `~/.dbt`. VS Code dbt extension compatibility. |
| P6.3: DVT error codes | MEDIUM | Proper DVT error classes (DVT100-DVT111). |
| P6.4: DVT event logging | MEDIUM | Sling/DuckDB operations via dbt's event system. |
| P6.5: `dvt init` template | MEDIUM | DVT-specific starter project. `.dvt/` in .gitignore. |

### Phase 7: DuckDB Connectivity to All Engines

| Item | Priority | Details |
|------|----------|---------|
| P7.1: DuckDB ATTACH research | HIGH | Inventory: postgres ✓, mysql ✓, sqlite ✓. Research: Snowflake, BigQuery, MSSQL, Oracle via extensions. |
| P7.2: `dvt show` for all engines | MEDIUM | ATTACH where possible, Sling-extract where not. |

### Phase 8: Advanced Features

| Item | Priority | Details |
|------|----------|---------|
| P8.1: Bucket materialization | MEDIUM | `config(target='s3_bucket', format='delta')` |
| P8.2: CDC extraction | LOW | Sling `change-capture` mode |
| P8.3: Virtual federation | LOW | `materialized='virtual'` via DuckDB ATTACH |

### Phase 9: Testing + Release

| Item | Priority | Details |
|------|----------|---------|
| P9.1: Unit tests | MEDIUM | All core modules |
| P9.2: Integration tests | MEDIUM | Full E2E test suite |
| P9.3: PyPI publish | MEDIUM | dvt-ce + dvt-adapters matching versions |
| P9.4: Documentation | LOW | README, getting started, migration guide |

---

## Architecture Summary (Current)

```
dvt-ce (provides dbt.* + dvt.*)
├── dbt/                           # Upstream dbt-core 1.9.10 (1 field added)
├── dvt/
│   ├── cli/
│   │   ├── __init__.py            # Resilient entry point
│   │   └── main.py                # Full CLI
│   ├── tasks/
│   │   ├── run.py                 # DvtRunTask — manages DvtCache + dispatch
│   │   ├── build.py               # DvtBuildTask
│   │   ├── seed.py                # DvtSeedTask — Sling CSV
│   │   ├── show.py                # DvtShowTask — DuckDB local
│   │   ├── debug.py               # DvtDebugTask — connection check
│   │   └── sync.py                # DvtSyncTask — env bootstrap
│   ├── config/
│   │   ├── source_connections.py  # Read connection: from sources.yml
│   │   └── target_resolver.py     # Execution path resolution
│   ├── extraction/
│   │   ├── connection_mapper.py   # 16 adapters → Sling URLs
│   │   ├── watermark_formatter.py # 13 dialects × 4 types
│   │   └── sling_client.py        # Sling wrapper (lazy import)
│   ├── runners/
│   │   ├── model_runner.py        # DvtModelRunner — pushdown vs extraction
│   │   └── seed_runner.py         # DvtSeedRunner — Sling CSV
│   ├── federation/
│   │   ├── dvt_cache.py           # Persistent DuckDB cache (.dvt/cache.duckdb)
│   │   └── duckdb_compute.py      # (legacy — replaced by dvt_cache.py)
│   └── sync/
│       ├── profiles_reader.py
│       ├── adapter_installer.py
│       ├── duckdb_extensions.py
│       ├── cloud_deps.py
│       └── sling_checker.py

dvt-adapters (provides dbt.adapters.* + dbt.include.*)
├── Framework: base, sql, contracts, catalogs, events, exceptions, record
├── 13 Engines: postgres, snowflake, bigquery, redshift, spark, databricks,
│               duckdb, sqlserver, mysql, mysql5, mariadb, oracle, fabric
└── Each: Python adapter code + SQL macros

.dvt/cache.duckdb (persistent, per-project)
├── Source tables: {source}__{table} (cached, shared across models)
├── Model results: __model__{name} (for incremental {{ this }})
└── Lifecycle: created on first run, persists, --full-refresh destroys
```

## Execution Paths

```
For each model:
  1. Resolve target (CLI --target > model config > profiles.yml default)
  2. Check all source().connection vs model.target
  3. If ALL local → PUSHDOWN (adapter SQL on target, standard dbt)
  4. If ANY remote → EXTRACTION:
     a. Sling extracts sources → .dvt/cache.duckdb
     b. Model SQL runs in DuckDB (DuckDB SQL dialect)
     c. Sling loads result → target
     d. Incremental: watermark from TARGET, delta-only extraction
```

---

*This document is updated after each development session.*
