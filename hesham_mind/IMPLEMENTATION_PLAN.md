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

### Phase 13.5: Extraction Fixes [DONE]

| Item | Status | Details |
|------|--------|---------|
| P13.5.1: Sling truncate mode | DONE | Non-incremental extraction uses `truncate` instead of `full-refresh` to preserve dependent views on the target. |
| P13.5.2: Cache append for incremental | DONE | Append-strategy incrementals INSERT INTO the DuckDB cache table instead of replacing — accumulates rows across runs. |
| P13.5.3: Sling error reporting | DONE | Raw Sling error included when clean error parsing falls through to generic "extraction failed". |
| P13.5.4: `dvt docs serve --host` | DONE | Added missing `--host` CLI parameter. |

### Phase 13.6: dvt retract [DONE]

| Item | Status | Details |
|------|--------|---------|
| P13.6.1: `dvt retract` command | DONE | Drops all models from targets in reverse DAG order. Full dbt selector support (`--select`, `--exclude`, `+model+`, `tag:`, etc.). |
| P13.6.2: CASCADE on DROP | DONE | Uses CASCADE for engines that support it (postgres, snowflake, oracle, redshift). |
| P13.6.3: DuckDB cache cleanup | DONE | Drops `__model__` tables from cache for retracted models. |
| P13.6.4: Multi-engine support | DONE | Connects to each model's target via native driver to execute DROP. |

### Phase 14: README + Branding + Init [DONE]

| Item | Status | Details |
|------|--------|---------|
| P14.1: GitHub/PyPI README | DONE | Mermaid architecture diagram, engine table with emojis, badges (PyPI, Python, Discord, License), credits (dbt-core, Sling free tier, DuckDB), Hesham LinkedIn, Apache 2.0 LICENSE file. |
| P14.2: DVT logo in docs | DONE | White-bg colorful swirl logo (7.png), 60px, cropped for fill. "Data Catalog" text bold 15px next to it. |
| P14.3: Docs branding | DONE | Page title "DVT Docs", meta tags updated, dbt references replaced. |
| P14.4: Engine-colored lineage | DONE | Source + model nodes colored by engine brand colors. Target/Engine fields in detail panels. |
| P14.5: `dvt init` rewrite | DONE | DVT-specific init: defaults project name to current dir, scaffolds in-place (no subdirectory), creates `~/.dvt/profiles.yml`, 13 adapter selection, no psycopg2 crash. |
| P14.6: Root README sync | DONE | Root `README.md` synced with `core/README.md`. GitHub shows DVT README on all branches. |

### Phase 14.5: Stability + PyPI Fixes [DONE]

| Item | Status | Details |
|------|--------|---------|
| P14.5.1: PyPI version fix | DONE | Old Spark versions (0.1.9–0.1.22) outranked new Sling versions. Fixed by publishing 0.1.23+ from new_dev branch. |
| P14.5.2: Graceful driver error | DONE | All commands show clean "Run dvt sync" message instead of raw Python traceback when database drivers are missing. |
| P14.5.3: Sling sslmode fix | DONE | `sslmode=prefer` mapped to `disable` in connection_mapper — Sling's pq driver doesn't support `prefer`/`allow`. Fixes seeds, extraction, debug on all platforms. |
| P14.5.4: `dvt sync` core deps | DONE | Sync now verifies and installs missing core deps (duckdb, pyarrow, sqlglot, sling) — catches partial installs from memory/network failures. |
| P14.5.5: `dvt sync` driver check | DONE | Checks actual database driver (psycopg2, not just dbt.adapters.postgres) — catches cases where adapter code exists but driver pip package is missing. |
| P14.5.6: `dvt debug` dual test | DONE | Tests BOTH adapter (native driver) AND Sling for each connection. Shows "Adapter: OK \| Sling: OK". |
| P14.5.7: Missing CLI decorators | DONE | `dvt test` was missing `@p.resource_type`/`@p.exclude_resource_type`. `dvt snapshot` was missing `@p.empty`. Audited all commands against dbt originals. |
| P14.5.8: dvt-adapters dependency | DONE | Changed `dvt-adapters>=0.2.1` to `>=0.1.23` in setup.py. |
| P14.5.9: setup.py author/URL | DONE | Changed from dbt Labs to Hesham Badawi + DVT GitHub URL. |
| P14.5.10: dvt-pro reserved | DONE | PyPI name reserved at 0.1.23. GitHub repo: heshamh96/dvt-pro (private). |

### Phase 15: Advanced Features

| Item | Priority | Status | Details |
|------|----------|--------|---------|
| P15.1: Bucket materialization | MEDIUM | | `config(target='s3_bucket', format='delta')` |
| P15.2: CDC extraction | LOW | | Sling `change-capture` mode (dvt-pro feature) |
| P15.4: Remote catalog enrichment | MEDIUM | DONE | Models on non-default targets now get column metadata. 68/70 models with columns (2 ephemeral = correct). |
| P15.5: DVT website | LOW | | Dedicated domain (dvt.dev or getdvt.com) — landing page, docs, pricing |

### Phase 16: Testing + Release

| Item | Priority | Details |
|------|----------|---------|
| P16.1: Unit tests | MEDIUM | Watermark formatter, connection mapper, target resolver, optimizer |
| P16.2: Integration test suite | MEDIUM | Automated E2E tests for all execution paths |
| P16.3: PyPI publish automation | MEDIUM | Skill-based publish: bump version, build, upload, push, sync branches |
| P16.4: GitHub release notes | LOW | Changelog, migration guide from dbt |

---

## VERSION HISTORY

| Version | Date | Highlights |
|---------|------|-----------|
| 0.1.34 | 2026-03-31 | Docs logo "Data Catalog", all stability fixes |
| 0.1.30 | 2026-03-30 | Missing CLI decorators fixed (test, snapshot) |
| 0.1.29 | 2026-03-30 | Sling sslmode=prefer→disable fix |
| 0.1.28 | 2026-03-30 | Graceful driver-missing error |
| 0.1.27 | 2026-03-30 | dvt sync core deps verification |
| 0.1.25 | 2026-03-30 | dvt init in-place + dvt debug dual test |
| 0.1.24 | 2026-03-30 | dvt init rewrite (DVT-specific) |
| 0.1.23 | 2026-03-30 | Version bump past old Spark releases on PyPI |
| 0.1.8 | 2026-03-25 | P13 docs lineage + retract + branding |
| 0.1.6 | 2026-03-22 | P13.2+P13.3 engine-colored lineage |
| 0.1.5 | 2026-03-18 | P12 source connection v2 |

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
