# DVT Adapter Compatibility Matrix

## Database Engines vs Adapter Packages

| Engine | Service | Container | Port | Adapter Package | Status | JDBC Driver |
|--------|---------|-----------|------|----------------|--------|------------|
| PostgreSQL 16 | postgres | db_postgres | 5432 | dbt-postgres | Supported | postgresql-42.7.3.jar |
| MySQL 8.0 | mysql | db_mysql | 3306 | dbt-mysql | Community | mysql-connector-j-9.2.0.jar |
| SQL Server 2022 | sqlserver | db_sqlserver | 1433 | dbt-sqlserver | Community | mssql-jdbc-12.8.1.jre11.jar |
| Oracle XE 21c | oracle | db_oracle | 1521 | dbt-oracle | Future | ojdbc11.jar |
| MariaDB 11 | mariadb | db_mariadb | 3307 | dbt-mysql | Community (same as MySQL) | mariadb-java-client-3.5.2.jar |
| Databricks | (cloud) | N/A | N/A | dbt-databricks | Supported | spark-databricks JDBC |
| Snowflake | (cloud) | N/A | N/A | dbt-snowflake | Supported | snowflake-jdbc-3.19.0.jar |

**Note**: Docker engines run in containers on a shared `db_network`. External engines (pg_dev, Snowflake, Databricks) are already running and don't need Docker management.

## Container Credentials (from .env)

| Engine | User | Password | Database |
|--------|------|----------|----------|
| PostgreSQL | devuser | Postgres_Dev_2026! | devdb |
| MySQL | devuser | MySql_Dev_2026! | devdb |
| SQL Server | sa | SqlServer_Dev_2026! | master (+ devdb after init) |
| Oracle | system | Oracle_Dev_2026! | XEPDB1 |
| MariaDB | devuser | MariaDb_Dev_2026! | devdb |

**Note**: Container PG (port 5432) is separate from the development PG (port 5433). Don't confuse them.

## Feature Support Matrix

| Feature | PostgreSQL | MySQL | SQL Server | Oracle | MariaDB | Databricks | Snowflake |
|---------|-----------|-------|------------|--------|---------|-----------|-----------|
| **Seeds** | | | | | | | |
| CSV seeds | YES | YES | YES | YES | YES | YES | YES |
| JSON seeds | YES | TBD | TBD | TBD | TBD | YES | YES |
| Parquet seeds | YES | TBD | TBD | TBD | TBD | YES | YES |
| **Materializations** | | | | | | | |
| table | YES | YES | YES | YES | YES | YES | YES |
| view | YES | YES | YES | YES | YES | YES | YES |
| incremental (append) | YES | TBD | TBD | TBD | TBD | YES | YES |
| incremental (merge) | YES | TBD | TBD | TBD | TBD | YES | YES |
| incremental (del+ins) | YES | TBD | TBD | TBD | TBD | YES | YES |
| ephemeral | YES | YES | YES | YES | YES | YES | YES |
| **DDL Contract** | | | | | | | |
| TRUNCATE + INSERT | YES | YES | YES | TBD | YES | YES | YES |
| DROP + CREATE + INSERT | YES | YES | YES | TBD | YES | YES | YES |
| **Federation** | | | | | | | |
| JDBC extraction | YES | YES | YES | TBD | YES | YES* | YES* |
| JDBC loading | YES | YES | YES | TBD | YES | YES* | YES* |
| Pipe extraction (CLI) | psql | mysql | bcp | TBD | mysql | N/A | N/A |
| Native Spark connector | N/A | N/A | N/A | N/A | N/A | YES | YES |

*Cloud targets (Databricks, Snowflake) use JDBC for federation. They are now tested directly in Type 4 adapter tests as external engines (disf_dev writable, sf_dev read-only, dbx_dev).

## profiles.yml Template for Containerized Engines

```yaml
AdapterTest:
  outputs:
    pg_docker:
      type: postgres
      host: localhost        # or postgres_test if inside Docker network
      port: 5432
      user: devuser
      password: Postgres_Dev_2026!
      database: devdb
      schema: public
      threads: 4

    mysql_docker:
      type: mysql
      host: localhost        # or mysql_test
      port: 3306
      user: devuser
      password: MySql_Dev_2026!
      database: devdb
      schema: devdb
      threads: 4

    mssql_docker:
      type: sqlserver
      host: localhost        # or mssql_test
      port: 1433
      user: sa
      password: SqlServer_Dev_2026!
      database: devdb
      schema: dbo
      threads: 4

    oracle_docker:
      type: oracle
      host: localhost        # or oracle_test
      port: 1521
      user: system
      password: Oracle_Dev_2026!
      database: XEPDB1
      schema: SYSTEM
      threads: 4

    mariadb_docker:
      type: mysql            # MariaDB uses same adapter
      host: localhost        # or mariadb_test
      port: 3307
      user: devuser
      password: MariaDb_Dev_2026!
      database: devdb
      schema: devdb
      threads: 4

  target: pg_docker
```

## Test Sequence Per Engine

### Docker Engines (postgres, mysql, mssql, oracle, mariadb)

```bash
# 1. Start container
docker compose up -d <engine>

# 2. Wait for health check
docker compose ps  # Should show "healthy"

# 3-6. Standard test steps (see below)
```

### External Engines (pg_dev, snowflake, databricks)

```bash
# No Docker steps — engine must already be running
# Skip directly to standard test steps
```

### Standard Test Steps (all engines)

```bash
# 1. Verify connection
dvt debug --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 2. Seed test data
dvt seed --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 3. Run models
dvt run --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 4. Full refresh (DDL contract: DROP+CREATE+INSERT)
dvt run --full-refresh --target <target> --profiles-dir test_project/Connections --project-dir test_project
```

## Cross-Engine Federation Test Models

These models test DVT's core differentiator: reading from one database engine and writing to another via Spark JDBC federation.

**Location**: `Testing_adapters_docker/test_project/models/federation/`

**Docker-to-Docker** (9 models):
| Model | Source | Target | What It Tests |
|-------|--------|--------|---------------|
| `pg_to_mysql.sql` | pg_docker | mysql_docker | PG JDBC extraction + MySQL JDBC load |
| `pg_to_mssql.sql` | pg_docker | mssql_docker | PG JDBC extraction + MSSQL JDBC load |
| `pg_to_oracle.sql` | pg_docker | oracle_docker | PG JDBC extraction + Oracle JDBC load |
| `pg_to_mariadb.sql` | pg_docker | mariadb_docker | PG JDBC extraction + MariaDB JDBC load |
| `mysql_to_pg.sql` | mysql_docker | pg_docker | MySQL JDBC extraction + PG JDBC load |
| `mssql_to_pg.sql` | mssql_docker | pg_docker | MSSQL JDBC extraction + PG JDBC load |
| `oracle_to_pg.sql` | oracle_docker | pg_docker | Oracle JDBC extraction + PG JDBC load |
| `mariadb_to_pg.sql` | mariadb_docker | pg_docker | MariaDB JDBC extraction |
| `cross_pg_mysql.sql` | pg_docker + mysql_docker | pg_docker | Multi-source Spark SQL join |

**Cross-infrastructure** (8 models):
| Model | Source | Target | What It Tests |
|-------|--------|--------|---------------|
| `pg_to_snowflake.sql` | pg_docker | disf_dev | Docker-to-cloud (Snowflake) |
| `pg_to_databricks.sql` | pg_docker | dbx_dev | Docker-to-cloud (Databricks) |
| `snowflake_to_pg.sql` | sf_dev (read) | pg_docker | Cloud-to-Docker (Snowflake) |
| `databricks_to_pg.sql` | dbx_dev | pg_docker | Cloud-to-Docker (Databricks) |
| `pg_dev_to_pg_docker.sql` | pg_dev | pg_docker | Cross-instance PG federation |
| `pg_docker_to_pg_dev.sql` | pg_docker | pg_dev | Cross-instance PG federation |
| `mysql_to_snowflake.sql` | mysql_docker | disf_dev | Docker-to-cloud (MySQL->SF) |
| `mysql_to_databricks.sql` | mysql_docker | dbx_dev | Docker-to-cloud (MySQL->DBX) |

### Federation test data flow

```
Source DB              Spark (local)           Target DB
  [PG]  --JDBC--->  [Extract to staging]  --JDBC--->  [MySQL/MSSQL/Oracle/MariaDB/SF/DBX]
  [MySQL] --JDBC--> [Extract to staging]  --JDBC--->  [PG/SF/DBX]
  [MSSQL] --JDBC--> [Extract to staging]  --JDBC--->  [PG]
  [Oracle] --JDBC-> [Extract to staging]  --JDBC--->  [PG]
  [MariaDB] ------> [Extract to staging]  --JDBC--->  [PG]
  [SF] --JDBC-----> [Extract to staging]  --JDBC--->  [PG]
  [DBX] --JDBC----> [Extract to staging]  --JDBC--->  [PG]
  [pg_dev] -------> [Extract to staging]  --JDBC--->  [pg_docker]
  [pg_docker] ----> [Extract to staging]  --JDBC--->  [pg_dev]
  [PG+MySQL] -----> [Join in Spark SQL]   --JDBC--->  [PG]
```

### Federation validation checks

For each federation model:
1. **Row count**: Target table has expected number of rows (4 rows where amount > 20)
2. **federation_path column**: Contains correct path identifier (e.g., 'pg_to_mysql')
3. **Data integrity**: All column values match source data
4. **DDL contract**: TRUNCATE+INSERT on default run, DROP+CREATE+INSERT on --full-refresh
5. **JDBC drivers**: Both source and target JDBC drivers loaded by Spark

### Running federation tests

```bash
# Run only federation tests (starts all required containers)
./run_adapter_tests.sh federation

# Run everything (8 engines + federation = full suite)
./run_adapter_tests.sh all

# Run only Docker engines
./run_adapter_tests.sh docker

# Run only external engines (pg_dev, snowflake, databricks)
./run_adapter_tests.sh external

# Run a specific engine
./run_adapter_tests.sh postgres
./run_adapter_tests.sh snowflake
./run_adapter_tests.sh databricks
```

## System Dependencies Per Adapter

| Adapter | Python Package | System Deps | Notes |
|---------|---------------|-------------|-------|
| dbt-postgres | dbt-postgres | libpq (usually bundled) | pip install just works |
| dbt-mysql | dbt-mysql | mysqlclient -> libmysqlclient-dev | `apt install libmysqlclient-dev` or `brew install mysql-client` |
| dbt-sqlserver | dbt-sqlserver | pymssql -> FreeTDS | `apt install freetds-dev` or `brew install freetds` |
| dbt-oracle | dbt-oracle | oracledb (thin mode) | No system deps in thin mode |
| dbt-databricks | dbt-databricks | None | pip install just works |
| dbt-snowflake | dbt-snowflake | None (uses snowflake-connector-python) | pip install just works |

## JDBC Driver Sources

| Driver | Maven Coordinates | Auto-downloaded by `dvt sync` |
|--------|------------------|-------------------------------|
| PostgreSQL | org.postgresql:postgresql:42.7.3 | YES |
| MySQL | com.mysql:mysql-connector-j:9.2.0 | YES |
| SQL Server | com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11 | YES |
| Oracle | com.oracle.database.jdbc:ojdbc11:23.7.0.25.01 | YES |
| MariaDB | org.mariadb.jdbc:mariadb-java-client:3.5.2 | YES |
| Snowflake | net.snowflake:snowflake-jdbc:3.19.0 | YES |
| Databricks | com.databricks:databricks-jdbc:2.6.40 | YES |
