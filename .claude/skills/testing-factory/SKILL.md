---
name: testing-factory
description: DVT Testing Factory — orchestrate all 6 test types
arguments:
  - name: type
    description: "Test type: unit, integration, cli, adapters, uat, os, all"
    required: true
  - name: subarg
    description: "Sub-argument: command name, distro, trial number, etc."
    required: false
---

# Skill: testing-factory

Master orchestrator for all DVT testing. Routes to the correct test workflow based on the requested test type.

## Usage

```
/testing-factory unit                  # Run unit tests (delegates to /test skill)
/testing-factory integration           # Run integration tests against local PG
/testing-factory cli                   # Run ALL CLI command tests
/testing-factory cli debug             # Run CLI tests for a specific command
/testing-factory adapters              # Run adapter compatibility tests against all containerized DBs
/testing-factory adapters postgres     # Run adapter tests for a specific DB engine
/testing-factory uat                   # Run full UAT E2E (delegates to /uat-e2e skill)
/testing-factory uat trial_18          # Run UAT on a specific trial
/testing-factory os                    # Run cross-OS tests against all distros
/testing-factory os ubuntu-24.04       # Run cross-OS test for a specific distro
/testing-factory all                   # Run everything in sequence
```

## Directory Layout

```
Testing_Factory/                                          # /Users/hex/Documents/My_Projects/DVT/Testing_Factory/
├── Testing_Playground/                                   # Type 5: UAT E2E trials
│   ├── trial_17_sync_uat/                                #   Latest trial with Coke_DB project
│   ├── trial_16_full_cli_e2e/                            #   Previous full E2E
│   └── ...                                               #   20 historical trials
├── Testing_DVT_cli_commands/                             # Type 3: CLI command tests
│   ├── conftest.py                                       #   Shared pytest fixtures
│   ├── run_all.sh                                        #   Run all CLI tests
│   └── test_<command>.py + test_<command>.sh              #   Per-command test pairs
├── Testing_adapters_docker/                              # Type 4: Adapter compatibility
│   ├── docker-compose.yml                                #   MySQL, PG, MSSQL, Oracle, MariaDB, Adminer
│   ├── .env                                              #   DB credentials
│   ├── test_project/                                     #   Minimal DVT project for adapter tests
│   └── run_adapter_tests.sh                              #   Orchestrator script
└── Testing_OSs_docker/                                   # Type 6: Cross-OS tests
    ├── docker-compose.yml                                #   All 7 distro containers
    ├── dvt_test_suite.sh                                 #   Test script copied into containers
    ├── ubuntu-22.04/Dockerfile                           #   Ubuntu 22.04 LTS
    ├── ubuntu-24.04/Dockerfile                           #   Ubuntu 24.04 LTS
    ├── debian-12/Dockerfile                              #   Debian Bookworm
    ├── rocky-9/Dockerfile                                #   Rocky Linux 9 (RHEL-compat)
    ├── alpine-3.19/Dockerfile                            #   Alpine (musl libc edge case)
    ├── fedora/Dockerfile                                 #   Fedora latest
    └── arch/Dockerfile                                   #   Arch Linux

dvt-core/                                                 # /Users/hex/Documents/My_Projects/DVT/dvt-core/
├── tests/unit/                                           # Type 1: Unit tests (pytest, no DB)
└── tests/functional/                                     # Type 2: Integration tests (pytest + PG)
```

## Path Constants

```
TESTING_FACTORY   = /Users/hex/Documents/My_Projects/DVT/Testing_Factory
TESTING_PLAYGROUND= /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_Playground
CLI_COMMANDS_DIR  = /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_DVT_cli_commands
ADAPTERS_DIR      = /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_adapters_docker
OS_DIR            = /Users/hex/Documents/My_Projects/DVT/Testing_Factory/Testing_OSs_docker
DVT_CORE          = /Users/hex/Documents/My_Projects/DVT/dvt-core
UNIT_TESTS        = /Users/hex/Documents/My_Projects/DVT/dvt-core/tests/unit
FUNCTIONAL_TESTS  = /Users/hex/Documents/My_Projects/DVT/dvt-core/tests/functional
```

## The 6 Test Types

See `reference/test-types.md` for detailed explanations of each test type, what it catches, and when to run it.

### Type 1: Unit Tests (`/testing-factory unit`)

**Location**: `dvt-core/tests/unit/`
**Runner**: `cd dvt-core/core && .venv/bin/python -m pytest ../tests/unit/ -v`
**Delegates to**: `/test` skill

Fast, isolated, no DB required. Tests individual functions, classes, contracts, parsers. Currently 2297 tests passing, 21 known failures (pre-existing).

Run after every code change. These are the first line of defense.

### Type 2: Integration Tests (`/testing-factory integration`)

**Location**: `dvt-core/tests/functional/`
**Runner**: `cd dvt-core/core && .venv/bin/python -m pytest ../tests/functional/ -v`
**Requires**: PostgreSQL running at localhost:5433

Tests the DVT runtime end-to-end in-process (not via CLI) using pytest fixtures. Inherited from upstream dbt-core. Tests model compilation, execution, and result verification against a real database.

**Current state**: Upstream tests exist but are not extended for DVT-specific features (federation, multi-target, computes.yml). Extending these is a TODO.

Run after changes to core runtime (compilation, execution, graph resolution).

### Type 3: CLI Command Tests (`/testing-factory cli [command]`)

**Location**: `Testing_Factory/Testing_DVT_cli_commands/`
**Runner**: `cd Testing_DVT_cli_commands && pytest -v` or `bash run_all.sh`

Systematic test of every DVT CLI command with all flags and argument combinations. Each command gets a paired `.sh` (manual quick-run) + `.py` (pytest with assertions).

See `reference/cli-commands.md` for the full command/flag matrix.

**Commands tested** (19 total):
`build`, `clean`, `clone`, `compile`, `debug`, `deps`, `docs generate`, `init`, `list`, `parse`, `retry`, `run`, `run-operation`, `seed`, `show`, `snapshot`, `source freshness`, `sync`, `test`

Each test covers:
- `--help` flag (exit 0, output contains usage)
- Basic invocation with valid project
- All documented flags
- Invalid inputs (wrong dir, missing profiles) -> non-zero exit
- Idempotency where applicable

Run after CLI changes, flag additions, or command wiring changes.

### Type 4: Adapter Compatibility Tests (`/testing-factory adapters [db]`)

**Location**: `Testing_Factory/Testing_adapters_docker/`
**Runner**: `bash run_adapter_tests.sh [db]` or `bash run_adapter_tests.sh federation`
**Requires**: Docker

Tests DVT against containerized database engines to verify adapter compatibility AND cross-engine federation. Each engine runs in its own Docker container on a shared `db_network`.

**Databases** (5 engines + 1 management UI, each in a separate container):
| Engine | Service Name | Container Name | Port | Adapter Package |
|--------|-------------|----------------|------|----------------|
| PostgreSQL 16 | postgres | db_postgres | 5432 | dbt-postgres |
| MySQL 8.0 | mysql | db_mysql | 3306 | dbt-mysql (community) |
| SQL Server 2022 | sqlserver | db_sqlserver | 1433 | dbt-sqlserver |
| Oracle XE 21c | oracle | db_oracle | 1521 | dbt-oracle (future) |
| MariaDB 11 | mariadb | db_mariadb | 3307 | dbt-mysql (community) |
| Adminer UI | adminer | db_adminer | 8080 | (management) |

**Phase A — Single-Engine Pushdown** (per engine):
1. `docker compose up -d <engine>` — start container
2. `dvt debug` — verify connection
3. `dvt seed` — load test data
4. `dvt run` — execute pushdown models (SQL on target DB)
5. Query DB to verify row counts and data integrity
6. `dvt run --full-refresh` — verify DDL contract (DROP+CREATE+INSERT)

**Phase B — Cross-Engine Federation** (DVT's key differentiator):
All containers must be running. Federation models read from one engine and write to another via Spark JDBC.

Test matrix:
| Source Engine | Target Engine | Model | Validates |
|--------------|--------------|-------|-----------|
| PostgreSQL | MySQL | `pg_to_mysql` | JDBC extraction from PG, JDBC load to MySQL |
| PostgreSQL | SQL Server | `pg_to_mssql` | JDBC load to MSSQL, type mapping |
| MySQL | PostgreSQL | `mysql_to_pg` | JDBC extraction from MySQL |
| SQL Server | PostgreSQL | `mssql_to_pg` | JDBC extraction from MSSQL |
| PostgreSQL + MySQL | PostgreSQL | `cross_pg_mysql` | Multi-source federation, Spark SQL join |
| MariaDB | PostgreSQL | `mariadb_to_pg` | MariaDB JDBC extraction |

Each federation test verifies:
- Spark session initializes correctly
- JDBC drivers for both source and target engines load
- Data is extracted from source, transformed in Spark, loaded to target
- Row counts match between source query and target table
- DDL contract holds (TRUNCATE+INSERT default, DROP+CREATE+INSERT with --full-refresh)

Run after adapter upgrades, JDBC driver updates, federation engine changes, or before adding support for a new database engine.

### Type 5: UAT E2E Tests (`/testing-factory uat [trial]`)

**Location**: `Testing_Factory/Testing_Playground/`
**Delegates to**: `/uat-e2e` skill (550-line protocol)

Comprehensive end-to-end test against real production-like databases (Postgres, Databricks, Snowflake). 10+ phases covering every DVT feature: federation, incremental strategies, staging, optimization, multi-target overrides, data validation.

This is the final quality gate before shipping. Produces a ship/no-ship recommendation.

**Targets**: pg_dev, dbx_dev, sf_dev (read-only), disf_dev (writable)

Run before any release or major merge to `uat`/`master`.

### Type 6: Cross-OS Tests (`/testing-factory os [distro]`)

**Location**: `Testing_Factory/Testing_OSs_docker/`
**Runner**: `bash run_os_tests.sh [distro]` or `docker compose up --build`
**Requires**: Docker, published dvt-core on PyPI (or local wheel)

Tests the published dvt-core package on multiple Linux distributions to catch OS-specific issues (system library differences, Python version compatibility, Java runtime availability).

**Distros** (7 targets):
| Distro | Base | Python | libc | Notes |
|--------|------|--------|------|-------|
| Ubuntu 22.04 | Debian | 3.12+ (deadsnakes) | glibc | LTS server standard |
| Ubuntu 24.04 | Debian | 3.12 (system) | glibc | Latest LTS |
| Debian 12 | - | 3.12+ (deadsnakes) | glibc | Minimal, common Docker base |
| Rocky Linux 9 | RHEL | 3.12+ (AppStream) | glibc | Enterprise/RHEL compat |
| Alpine 3.19 | - | 3.12 (system) | **musl** | Edge case: musl libc |
| Fedora | - | 3.12+ (system) | glibc | Cutting-edge packages |
| Arch | - | 3.12+ (system) | glibc | Rolling release |

Each container:
1. Installs Python 3.12+, Java 17, pip
2. `pip install dvt-core` (from PyPI or local wheel)
3. `pip install dbt-postgres` (and other adapters)
4. Copies Coke_DB test project
5. Connects to Testing_adapters_docker containers (shared Docker network)
6. Runs: `dvt debug`, `dvt sync`, `dvt seed`, `dvt run`, `dvt run --full-refresh`
7. Validates data matches macOS baseline

Run before publishing a new dvt-core version to PyPI.

## Test Pyramid

```
                    /\
                   /  \          Type 5: UAT E2E (manual, 4 real targets)
                  / 5  \         Type 6: Cross-OS (Docker, published pkg)
                 /------\
                /   6    \       Type 4: Adapter Compat (Docker DBs)
               / 4     3  \     Type 3: CLI Commands (all flags)
              /------------\
             /    2         \    Type 2: Integration (pytest + real DB)
            /       1        \   Type 1: Unit Tests (fast, isolated)
           /------------------\
```

**Run frequency**:
- Type 1 (unit): Every code change
- Type 2 (integration): After runtime changes
- Type 3 (CLI): After CLI/flag changes
- Type 4 (adapters): After adapter upgrades
- Type 5 (UAT): Before releases
- Type 6 (cross-OS): Before PyPI publish

## Workflow: `/testing-factory all`

Runs all test types in order (bottom of pyramid first):

```bash
# 1. Unit tests
cd dvt-core/core && .venv/bin/python -m pytest ../tests/unit/ -v

# 2. Integration tests
cd dvt-core/core && .venv/bin/python -m pytest ../tests/functional/ -v

# 3. CLI command tests
cd Testing_Factory/Testing_DVT_cli_commands && pytest -v

# 4. Adapter compatibility tests
cd Testing_Factory/Testing_adapters_docker && bash run_adapter_tests.sh

# 5. UAT E2E (delegate to /uat-e2e skill with latest trial)
# Create new trial via /trial skill, then run /uat-e2e

# 6. Cross-OS tests
cd Testing_Factory/Testing_OSs_docker && docker compose up --build
```

Stop at the first type that fails. Fix before proceeding up the pyramid.

## Critical Rules

1. **sf_dev is READ ONLY** — never write to it. Use disf_dev for writable Snowflake.
2. **DDL contract**: `dvt run` = TRUNCATE+INSERT; `dvt run --full-refresh` = DROP+CREATE+INSERT.
3. **No column name sanitization** — use proper quoting for all dialects.
4. **No fallbacks** — one method, works right.
5. **Test data must be deterministic** — use ORDER BY + LIMIT for traceable verification.
6. **Record everything** — all findings go to `findings/` directories.
