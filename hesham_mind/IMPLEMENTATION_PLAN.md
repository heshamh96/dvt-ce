# DVT Implementation Plan — Shared Memory

> **Last Updated:** March 2026  
> **Status:** Living document — updated after each phase

---

## Foundation

- **dvt-ce**: dbt-core 1.9.10 renamed to `dvt-ce`. Provides `dbt.*` + `dvt.*`.
- **dvt-adapters**: 13 engine adapters in one package. Provides `dbt.adapters.*` + `dbt.include.*`.
- **Test projects:**
  - `trial_19_dvt_ce_pypi/Coke_DB` — original 12-model test (Docker DBs)
  - `trial_20_full_coverage/Coke_DB` — full 68-model test (Docker + remote SF/PG/DBX)
- **Both packages:** local editable via `pyproject.toml` + `[tool.uv.sources]`
- **Profiles:** `~/.dvt/profiles.yml` (DVT-first, `~/.dbt` fallback)

---

## COMPLETED

### Phase 0: Scaffold + Sync [DONE]
### Phase 1: Core Extraction Pipeline [DONE]
### Phase 2: Seeds, Debug, Show [DONE]
### Phase 3: Package Consolidation [DONE]
### Phase 4: Persistent DuckDB Cache + Incremental [DONE]
### Phase 5: Federation Optimizer [DONE]
### Phase 6: Non-Default Pushdown + Polish [DONE]
### Phase 7: DuckDB Connectivity [DONE]
### Phase 8.5: dvt show --select [DONE]

### E2E Test Results

**Trial 19 (scoped — 12 models):** 12/12 PASS (both run 1 + run 2)
**Trial 20 (full DAG — 68 models):** Many failures exposed (see Feedback section below)

---

## USER FEEDBACK (v1) — CRITICAL ITEMS

From `hesham_tests/Feedback_v1/design_and_branding_feedback.md`:

### F1: dvt --version (BRANDING)
**Problem:** Shows "dbt-core", "Your version of dbt-core is out of date", links to getdbt.com. Lists adapter versions from PyPI, not from dvt-adapters.
**Fix needed:**
- Replace all "dbt-core" branding with "dvt-ce"
- Show DVT version only, no "update available" against PyPI dbt-core
- Adapter versions should come from dvt-adapters (one version for all)
- Link to `https://github.com/heshamh96/dvt-ce`
- Suppress oracle adapter "thin mode" warning and thrift SSL warning

### F2: dvt debug (UX)
**Problem:** Sling output is noisy, connection URLs exposed, inconsistent formatting.
**Fix needed:**
- Use 🟩 `[OK]` / 🟥 `[FAIL]` symbols
- Suppress ALL Sling output (connection URLs, temp file paths, CLI banners)
- User should NOT know Sling is involved — just show adapter connectivity status
- Clean, consistent formatting per connection

### F3: dvt sync (UX)
**Problem:** Functional but poorly formatted. Adapter warnings leak through.
**Fix needed:**
- Use 🟩/🟥 symbols for status
- Suppress oracle "thin mode" and thrift SSL warnings
- Clean formatting

### F4: dvt seed (FUNCTIONAL + UX)
**Problem:** Sling output is noisy. 3/11 seeds FAIL on type inference (dirty CSV data: "1.25%", "_4").
**Fix needed:**
- Suppress ALL Sling output during seeds — show only dbt-style "OK loaded seed file" / "ERROR loading seed file"
- Fix type inference: either pass `adjust_column_type: true` to Sling, or default columns to text/varchar (like dbt's agate loader does), or use `SLING_SAMPLE_SIZE` env var
- On error: show clean DVT error message, not raw Sling/pq errors
- The user should feel at home (dbt-like output)

### F5: dvt run (FUNCTIONAL + UX)
**Problem:** Sling output floods the terminal. Many models fail. Output is not dbt-like.
**Fix needed:**
- **UX:** Suppress ALL Sling/DuckDB output during model execution. The extraction path (source→Sling→DuckDB→Sling→target) is a BLACK BOX. Show only dbt-style model output ("OK created sql table model", "ERROR creating sql table model").
- **Functional:** Many failures in trial_20 are from:
  - Seeds that failed to load (cascading failures for models that depend on them)
  - Source tables not existing on `pg_dev` (the default target is `pg_dev` port 5433, not `pg_docker` port 5432)
  - Models with syntax errors for their target engines
- Need to fix Coke_DB models + seeds before re-testing

### F6: --target Philosophy (ARCHITECTURE)
**Problem:** `--target` can switch to a different adapter type, causing pushdown models to fail with syntax errors.
**Rule (added to DVT_RULES.md):**
- `--target` switches environments (same engine), NOT engines
- Pushdown models use target dialect — work on same engine type only
- Extraction models use DuckDB dialect — work on any engine
- DVT warns (DVT007) when `--target` changes adapter type, doesn't block
- This is the DVT philosophy: two dialects coexist in one project

---

## UPCOMING PHASES

### Phase 10: Seed Robustness + Full DAG [DONE]

| Item | Status | Details |
|------|--------|---------|
| P10.1: Adapter DROP CASCADE before Sling seed | DONE | Adapter drops table with CASCADE (removes dependent views), then Sling full-refresh creates clean table. |
| P10.2: Snake_case column normalization | DONE | `column_casing: snake` in Sling. CSV `Customer Code` → `customer_code` in DB. |
| P10.3: adjust_column_type for type inference | DONE | Sling auto-fixes type inference failures. Dirty CSV data loads correctly. |
| P10.4: SLING_LOADED_AT_COLUMN=false globally | DONE | No `_sling_loaded_at` metadata column on any engine (prevents Oracle CLOB issues). |
| P10.5: Cross-target seeding | DONE | Tested: pg_dev, pg_docker, mysql, oracle, mssql, mariadb, snowflake, databricks (all PASS). |
| P10.6: Large seed performance | DONE | 1M rows: 136K r/s (PG local), 19K r/s (Snowflake), 18K r/s (Databricks). |
| P10.7: Fix Coke_DB models | DONE | Snake_case columns, amount > '20' (string comparison), backtick→DuckDB, adapter.quote→lowercase, f_country cache naming, non-default target schema. |
| P10.8: Full DAG run | DONE | Trial 20: 65/68 PASS (then 68/68 after b_f_dept_acct_officer fix). |

### Phase 11: UX/Branding Overhaul [DONE]

Sling is invisible. No mention of Sling or DuckDB in user-facing output.
Developed on `feature/ux-branding-overhaul` branch, merged to `new_dev`.

| Item | Status | Details |
|------|--------|---------|
| P11.1: Suppress Sling in `dvt run` | DONE | `return_output=True` captures Sling output. One dbt-like line per model: `[SELECT 4 in 6s]`. |
| P11.2: Suppress Sling in `dvt seed` | DONE | Same treatment. One line per seed: `[INSERT 469 in 3s]`. |
| P11.3: `logs/dvt.log` | DONE | Renamed `dbt.log` → `dvt.log`. All details (Sling, debug) in one file. |
| P11.4: Clean error messages | DONE | `Model 'name' (target: x, adapter: y): error. Check logs/dvt.log.` |
| P11.5: `dvt --version` | DONE | Shows `dvt-ce: 0.1.5`, `dvt-adapters: 0.2.1`, GitHub link. No dbt branding. |
| P11.6: `dvt debug` emoji | DONE | 🟩/🟥 per connection. Sling output suppressed. |
| P11.7: `dvt sync` emoji | DONE | 🟩/🟥 per adapter/extension. |
| P11.8: DVT007 `--target` warning | DONE | Warns when adapter type changes. |
| P11.9: Row counts in output | DONE | Seeds show `INSERT N`, extraction shows `SELECT N`. |

### Phase 12: Source Connection v2 + Parse Validation [DONE]

`connection:` is optional for sources on the default target's adapter type (backward compatible with dbt).

| Item | Status | Details |
|------|--------|---------|
| P12.1: `connection:` optional for default target type | DONE | Sources without `connection:` are local (follow --target). No DVT100 error. |
| P12.2: DVT113 same-type+host:port warning | DONE | Same adapter type AND same host:port = redundant connection. Different port = different instance (allowed). |
| P12.3: Parse-time target change detection | DONE | `.dvt/state.json` stores last default target (name, type, hostname:port). Compared on each parse. |
| P12.4: DVT008 dialect migration warning | DONE | Warns when adapter type changes between parses. |
| P12.5: DVT009 instance switch warning | DONE | Warns when same type but different hostname:port. |
| P12.6: Hostname includes port | DONE | `host:port` for postgres/mysql/oracle/etc. `account` for Snowflake. `project` for BigQuery. |

### Phase 13: dvt docs (Catalog + Cross-Engine Lineage) [DONE]

| Item | Priority | Status | Details |
|------|----------|--------|---------|
| P13.1: Multi-connection catalog | HIGH | DONE | `dvt docs generate` queries each source's connection for schema/column info via native drivers. |
| P13.2: Cross-engine lineage | HIGH | DONE | Lineage graph colors source nodes by engine type (postgres=blue, snowflake=cyan, mysql=blue, oracle=red, etc.). Engine name shown in node labels. |
| P13.3: Connection badges in docs | MEDIUM | DONE | Source detail panel shows "Connection" field. Source list shows connections. Graph labels include engine badge. Manifest enriched with dvt_adapter_type for frontend. |

### Phase 14: README + Documentation

| Item | Priority | Details |
|------|----------|---------|
| P14.1: GitHub README (master branch) | HIGH | Comprehensive README for the Sling+DuckDB architecture. Getting started, installation, examples, philosophy. |
| P14.2: PyPI description | MEDIUM | Update setup.py long_description for PyPI listing. |
| P14.3: Create README update skill | LOW | Auto-generates README from hesham_mind/ docs when merging to master. |

### Phase 15: Advanced Features

| Item | Priority | Details |
|------|----------|---------|
| P15.1: Bucket materialization | MEDIUM | `config(target='s3_bucket', format='delta')` |
| P15.2: CDC extraction | LOW | Sling `change-capture` mode |
| P15.3: Virtual federation | LOW | `materialized='virtual'` via DuckDB ATTACH |

### Phase 16: Testing + Release

| Item | Priority | Details |
|------|----------|---------|
| P16.1: Unit tests | MEDIUM | Watermark formatter, connection mapper, target resolver, optimizer |
| P16.2: Integration test suite | MEDIUM | Automated E2E tests for all execution paths |
| P16.3: PyPI publish (clean release) | MEDIUM | dvt-ce + dvt-adapters with all UX fixes |
| P16.4: GitHub release notes | LOW | Changelog, migration guide from dbt |

---

## DVT Philosophy

### Two Dialects, One Project

DVT projects have two types of SQL:

1. **Target dialect** (pushdown models) — written for the target engine. Uses engine-specific features (Snowflake QUALIFY, Postgres JSONB, etc.). Runs directly on the target via adapter.

2. **DuckDB dialect** (extraction models) — written in DuckDB SQL. Engine-agnostic. Runs in DuckDB cache. Result loaded to any target via Sling.

### `--target` Switches Environments, Not Engines

- `dvt run --target prod_snowflake` → switches from dev to prod Snowflake. Same dialect. Works.
- `dvt run --target mssql_docker` → switches from Postgres to SQL Server. Pushdown models break. Expected.
- DVT warns (DVT007) but doesn't block — the user might know what they're doing.

### Sling is Invisible

The user should NEVER see Sling output, URLs, or errors. The extraction path (source→Sling→DuckDB→Sling→target) is a black box. DVT shows only dbt-like output. Sling details are logged at DEBUG level for troubleshooting.

### Seeds Must Be Robust

Sling seed loading must handle dirty CSV data as gracefully as dbt's agate loader. Default to text/varchar. Never crash on type inference.

---

## Architecture Summary

```
Execution flow:
  1. dbt compiles model (Jinja → SQL in target dialect)
  2. DVT detects remote sources (source.connection != model.target)
  3. If pushdown: adapter executes SQL on target (standard dbt)
  4. If extraction:
     a. Federation optimizer decomposes → per-source queries
     b. Sling extracts sources → .dvt/cache.duckdb (INVISIBLE)
     c. DuckDB executes model SQL (INVISIBLE)
     d. Sling loads result → target (INVISIBLE)
     e. User sees only: "OK created sql table model" (like dbt)

Paths:
  Default pushdown:     source.connection == default target → adapter SQL
  Non-default pushdown: target override, same adapter type → DuckDB cache → target
  Extraction:           source.connection != model target → DuckDB cache → target

Cache: .dvt/cache.duckdb (persistent, per-project, thread-safe)
  Sources: {source}__{table} (SELECT *, shared across models)
  Results: __model__{name} (for incremental {{ this }})
```

---

*Updated after each development session.*
