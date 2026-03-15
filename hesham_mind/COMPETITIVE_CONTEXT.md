# DVT Competitive Context

## What DVT Steals From Each Tool

### From dbt
- Declarative SQL models with Jinja templating
- DAG-based execution with dependency resolution
- `ref()` and `source()` functions for lineage
- Testing framework (unique, not_null, relationships, custom)
- Documentation generation with lineage graphs
- Incremental materialization patterns
- Package ecosystem
- profiles.yml / dbt_project.yml config system
- CLI experience: run, build, test, compile, seed, docs, deps

### From Sling
- High-performance Go-based data streaming (memory-efficient, single binary)
- 30+ database connectors without JVM/Spark overhead
- Incremental extraction via update_key watermarks
- CDC via database transaction logs (MySQL binlog, Postgres WAL)
- Replication YAML for declarative EL configuration
- Native bulk loading (COPY, bcp, sqlldr) for fast seed loading
- Schema evolution (auto-add new columns)
- Inline row-level transforms during extraction
- Wildcard table discovery (`schema.*`)
- Connection introspection (`sling conns discover`)

### From Denodo
- Federation query optimization (predicate pushdown, column pruning)
- Cost-based source routing (query the cheapest source)
- Caching strategy (extraction tables as cached copies with TTL)
- Universal semantic layer concept (sources appear as local tables)
- The philosophy: minimize data movement, query data where it lives

### From Spark
- Catalyst-style query optimization applied to extraction queries
- Delta Lake as the default format for bucket materialization
- The idea of adaptive execution (choose extraction strategy at runtime)
- Partition-aware extraction for large tables (future)

### From Ab Initio
- Checkpoint/restart: failed extractions don't require re-extracting successful ones
- Data quality as a pipeline concern (dbt tests after extraction)
- Lineage and impact analysis across the full pipeline
- The ambition to handle enterprise-scale on a single node

### From Informatica
- Mapping/session/workflow separation (model = what, profile = how, schedule = when)
- Pushdown optimization toggle (DVT: pushdown vs cross-target per model)
- Reusable transformation patterns (dbt packages/macros)

### From Matillion
- Pre-built transformation components → DVT ships common macros (SCD2, dedup, pivot)
- Environment promotion (dev → staging → prod via profiles.yml targets)

### From DuckDB
- ATTACH for zero-copy cross-database queries (dvt show)
- Extension ecosystem for local development
- In-process analytics without infrastructure
- Runs on a laptop philosophy

## DVT's Unique Position

No existing tool combines all of these:

| Capability | dbt | Sling | Denodo | Spark | DVT |
|-----------|-----|-------|--------|-------|-----|
| Declarative SQL models | YES | no | no | no | YES |
| DAG with lineage | YES | no | yes | no | YES |
| Cross-engine data movement | no | YES | virtual | yes | YES |
| Incremental extraction | no | YES | cache | yes | YES |
| CDC | no | YES | no | no | YES |
| Federation optimization | no | no | YES | partial | YES |
| Local dev (no infra) | partial | yes | no | no | YES |
| Bucket as target | no | YES | no | yes | YES |
| Delta Lake output | no | yes | no | YES | YES |
| Runs on a laptop | yes | yes | no | no | YES |
| Open source | yes | yes | no | yes | YES |
| SQL-first (no GUI) | yes | yes | no | partial | YES |

## DVT Value Proposition (One Sentence)

DVT is a dbt-compatible tool that lets you write SQL models that reference sources
on any database, automatically handles cross-engine data movement via Sling,
pushes transformations down to the target database, and materializes results
to any target — databases or cloud buckets — all from a laptop.

## What DVT Does NOT Try To Be

- NOT a distributed compute engine (no cluster, no horizontal scaling)
- NOT a GUI tool (CLI-first, code-first)
- NOT a streaming platform (batch and micro-batch only, via Sling CDC)
- NOT a replacement for a data warehouse (the warehouse IS the compute)
- NOT an orchestrator (use Airflow/Dagster/Prefect to schedule `dvt run`)
- NOT a data catalog (use DataHub/OpenMetadata/Atlan for that)

## Target Users

1. **Data engineers** who use dbt but need to pull from multiple source databases
2. **Analytics engineers** who want dbt's simplicity with cross-engine capability
3. **Small teams** who can't afford Informatica/Denodo/Matillion licenses
4. **Teams migrating** between warehouses who want portable transformation logic
5. **Startups** that need a full data stack on a laptop before scaling to cloud
