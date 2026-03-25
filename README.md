# DVT — Data Virtualization Tool

**DVT** is a cross-engine data transformation tool built on top of dbt-core. Write SQL models that reference sources on **any database**, and DVT automatically handles cross-engine data movement and materializes results to **any target**.

No custom connectors. No complex config. Just SQL.

---

## Architecture

DVT extends dbt with three execution paths:

```
┌─────────────────────────────────────────────────────────────┐
│                      DVT Execution Engine                    │
│                                                              │
│  ┌──────────────┐   Same engine?   ┌──────────────────────┐ │
│  │  Model SQL   │───── YES ───────▶│  Adapter Pushdown    │ │
│  │              │                   │  (standard dbt)      │ │
│  │  source A    │                   └──────────────────────┘ │
│  │  source B    │                                            │
│  │  ...         │   Different       ┌──────────────────────┐ │
│  │              │── engines? ──────▶│  Federation Path     │ │
│  └──────────────┘   YES             │                      │ │
│                                     │  Sling extracts ──┐  │ │
│                                     │  DuckDB joins   ──┤  │ │
│                                     │  Sling loads    ──┘  │ │
│                                     └──────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### Pushdown Path (Same Engine)
When all sources and the target are on the same engine, DVT behaves identically to dbt — the adapter executes SQL directly on the database.

### Extraction Path (Cross-Engine Federation)
When sources live on different engines than the target:

1. **Extract**: [Sling](https://slingdata.io) pulls source data from each remote engine
2. **Cache**: Data is cached in a local [DuckDB](https://duckdb.org) instance (`.dvt/cache.duckdb`)
3. **Transform**: DuckDB executes the model SQL (joins, aggregations, etc.)
4. **Load**: Sling pushes the result to the target engine

This is invisible to the user — DVT shows standard dbt-like output.

---

## Supported Engines

DVT supports **13 database engines** through [dvt-adapters](https://pypi.org/project/dvt-adapters/) (one package, all engines):

| Engine | Type | Pushdown | Extract From | Load To |
|--------|------|----------|-------------|---------|
| PostgreSQL | OLTP | Yes | Yes | Yes |
| MySQL | OLTP | Yes | Yes | Yes |
| MariaDB | OLTP | Yes | Yes | Yes |
| SQL Server | OLTP | Yes | Yes | Yes |
| Oracle | OLTP | Yes | Yes | Yes |
| Snowflake | Cloud DW | Yes | Yes | Yes |
| Databricks | Cloud DW | Yes | Yes | Yes |
| BigQuery | Cloud DW | Yes | Yes | Yes |
| Redshift | Cloud DW | Yes | Yes | Yes |
| DuckDB | Embedded | Yes | Yes | Yes |
| Spark | Distributed | Yes | Yes | Yes |
| Fabric | Cloud DW | Yes | Yes | Yes |
| MySQL 5 | Legacy | Yes | Yes | Yes |

Any source can feed into any target. DVT handles the data movement automatically.

---

## Installation

```bash
pip install dvt-ce dvt-adapters
```

Or with [uv](https://docs.astral.sh/uv/) (recommended):

```bash
uv add dvt-ce dvt-adapters
```

### Environment Bootstrap

After installing, run `dvt sync` to automatically install:
- Database drivers for all configured connections
- DuckDB extensions (httpfs, postgres, mysql, etc.)
- Cloud SDKs (for Snowflake, BigQuery, Databricks)
- Sling binary (for data extraction/loading)

```bash
dvt sync
```

`dvt sync` is self-healing — it can run even when dbt imports are broken, and will fix the environment.

---

## Quick Start

```bash
# 1. Initialize a new project
dvt init my_project
cd my_project

# 2. Configure connections in ~/.dvt/profiles.yml (see below)

# 3. Bootstrap the environment
dvt sync

# 4. Test all connections
dvt debug

# 5. Load seed data
dvt seed

# 6. Run all models
dvt run

# 7. Generate documentation with cross-engine lineage
dvt docs generate
dvt docs serve
```

---

## Configuration

### Profiles (`~/.dvt/profiles.yml`)

DVT looks for profiles in `~/.dvt/` first, then falls back to `~/.dbt/`.

```yaml
my_project:
  target: pg_dev                    # Default target
  outputs:
    # ─── PostgreSQL ───
    pg_dev:
      type: postgres
      host: localhost
      port: 5432
      user: analyst
      password: secret
      dbname: warehouse
      schema: public

    # ─── Snowflake ───
    sf_prod:
      type: snowflake
      account: my-account
      user: loader
      password: secret
      database: ANALYTICS
      schema: PUBLIC
      warehouse: COMPUTE_WH

    # ─── MySQL ───
    mysql_prod:
      type: mysql
      host: mysql.example.com
      port: 3306
      user: reader
      password: secret
      database: crm
      schema: crm

    # ─── Databricks ───
    dbx_dev:
      type: databricks
      host: my-workspace.cloud.databricks.com
      http_path: /sql/1.0/warehouses/abc123
      token: dapi...
      catalog: main
      schema: analytics
```

### Sources (`models/sources.yml`)

The `connection:` field tells DVT which engine each source lives on:

```yaml
version: 2

sources:
  # Sources on PostgreSQL (default target — no connection: needed)
  - name: app_db
    schema: public
    tables:
      - name: users
      - name: orders
      - name: products

  # Sources on MySQL (remote — needs connection:)
  - name: crm
    connection: mysql_prod          # Maps to profiles.yml output name
    schema: crm
    tables:
      - name: customers
      - name: contacts

  # Sources on Snowflake (remote)
  - name: marketing
    connection: sf_prod
    schema: PUBLIC
    tables:
      - name: campaigns
      - name: ad_spend

  # Sources on Databricks (remote)
  - name: data_lake
    connection: dbx_dev
    schema: raw
    tables:
      - name: events
      - name: sessions
```

**Rule**: Sources on the default target's engine type don't need `connection:` — they follow `--target` automatically. Remote sources (different engines) must have `connection:`.

### Model Configuration

```sql
-- models/dim_customer_360.sql
-- Cross-engine join: Postgres + MySQL + Snowflake → Databricks
{{
    config(
        materialized='table',
        target='dbx_dev'               -- Materialize to Databricks
    )
}}

SELECT
    u.user_id,
    u.email,
    c.customer_name,
    c.lifetime_value,
    m.total_ad_spend,
    e.session_count
FROM {{ source('app_db', 'users') }} u                -- Postgres
LEFT JOIN {{ source('crm', 'customers') }} c           -- MySQL
    ON u.email = c.email
LEFT JOIN {{ source('marketing', 'ad_spend') }} m      -- Snowflake
    ON u.user_id = m.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) as session_count
    FROM {{ source('data_lake', 'sessions') }}         -- Databricks
    GROUP BY user_id
) e ON u.user_id = e.user_id
```

DVT detects the 4 different engines, extracts all sources to DuckDB, executes the join, and loads the result to Databricks. The user sees standard dbt output.

### Incremental Models

Incremental models work across engines with watermark-based extraction:

```sql
{{
    config(
        materialized='incremental',
        incremental_strategy='append',
        target='sf_prod'
    )
}}

SELECT *
FROM {{ source('app_db', 'orders') }}
{% if is_incremental() %}
WHERE order_date > (SELECT MAX(order_date) FROM {{ this }})
{% endif %}
```

DVT reads the watermark from the target, extracts only new rows from the source, and appends them.

---

## Two Dialects in One Project

A DVT project naturally contains SQL in two dialects:

| Execution Path | SQL Dialect | When |
|---------------|-------------|------|
| **Pushdown** | Target's native SQL (Snowflake SQL, PostgreSQL, etc.) | Source and target on same engine |
| **Extraction** | DuckDB SQL (Postgres-like, universal) | Sources on different engines than target |

Both coexist in the same project. The dialect is determined automatically by the execution path.

---

## Commands Reference

### Core Commands

| Command | Description |
|---------|-------------|
| `dvt run` | Compile and execute models against targets |
| `dvt run --full-refresh` | Rebuild all models from scratch |
| `dvt run --select model_name` | Run specific models |
| `dvt run --select +model_name` | Run model and all ancestors |
| `dvt run --select tag:daily` | Run models by tag |
| `dvt build` | Seeds + models + snapshots + tests in DAG order |
| `dvt seed` | Load CSV seed files via Sling (10-100x faster than dbt) |
| `dvt test` | Run data tests |
| `dvt compile` | Compile SQL without executing |

### DVT-Specific Commands

| Command | Description |
|---------|-------------|
| `dvt sync` | Self-healing environment bootstrap — installs all drivers, DuckDB extensions, Sling binary, and cloud SDKs |
| `dvt debug` | Test all connections with clean status output |
| `dvt debug --target sf_prod` | Test a specific connection |
| `dvt show --select model_name` | Query model locally via DuckDB (no target needed) |
| `dvt retract` | Drop all models from targets in reverse DAG order |
| `dvt retract --select +model` | Drop a model and all its ancestors |
| `dvt clean` | Remove build artifacts and DuckDB cache |

### Documentation Commands

| Command | Description |
|---------|-------------|
| `dvt docs generate` | Generate cross-engine catalog with engine-colored lineage graph |
| `dvt docs serve` | Serve documentation website locally |

The documentation UI shows:
- Engine-colored nodes in the lineage graph (each engine has its brand color)
- Connection badges on source and model nodes
- Column metadata from all engines with native data types
- Target and engine info in model detail panels

### Passthrough Commands (Standard dbt)

| Command | Description |
|---------|-------------|
| `dvt snapshot` | Run snapshot models |
| `dvt deps` | Install dbt packages |
| `dvt init` | Initialize a new project |
| `dvt list` | List resources in the project |

---

## DuckDB Cache

DVT maintains a persistent DuckDB cache at `.dvt/cache.duckdb`:

- **Source tables**: `{source}__{table}` — shared across models, reused between runs
- **Model results**: `__model__{name}` — for incremental `{{ this }}` references
- `dvt run --full-refresh` rebuilds the cache
- `dvt clean` deletes the `.dvt/` directory entirely

---

## `--target` Philosophy

`--target` switches between **same-engine environments**, not between engines:

```bash
# Correct: switch between Snowflake environments
dvt run --target dev_snowflake
dvt run --target prod_snowflake

# Risky: switching engine types breaks pushdown models
dvt run --target dev_snowflake    # Snowflake SQL
dvt run --target mysql_docker     # MySQL can't parse Snowflake SQL
```

Extraction models (DuckDB SQL) are unaffected by `--target` changes. Pushdown models are written in the target's dialect and will fail if the engine type changes.

---

## dbt Compatibility

When using a single adapter with no cross-engine references, DVT works identically to dbt. All dbt projects are valid DVT projects. DVT adds federation capabilities on top — it never removes dbt functionality.

---

## Links

- **PyPI**: [dvt-ce](https://pypi.org/project/dvt-ce/) | [dvt-adapters](https://pypi.org/project/dvt-adapters/)
- **GitHub**: [dvt-ce](https://github.com/heshamh96/dvt-ce) | [dvt-adapters](https://github.com/heshamh96/dvt-adapters)
- **Issues**: [Report a bug](https://github.com/heshamh96/dvt-ce/issues)

## License

DVT is built on top of [dbt-core](https://github.com/dbt-labs/dbt-core) (Apache 2.0).
