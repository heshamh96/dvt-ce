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

**Note**: Each database engine runs in its own Docker container on a shared `db_network`. The service names (postgres, mysql, etc.) are the hostnames when connecting from within the Docker network.

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

*Cloud targets (Databricks, Snowflake) use JDBC for federation but are not in the Docker test set. They are tested via UAT E2E (Type 5).

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

For each database engine, run:

```bash
# 1. Start container
docker compose up -d <engine>

# 2. Wait for health check
docker compose ps  # Should show "healthy"

# 3. Verify connection
dvt debug --profiles-dir test_project/Connections --project-dir test_project

# 4. Seed test data
dvt seed --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 5. Run models
dvt run --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 6. Verify data (query via adapter or CLI tool)
# e.g., psql, mysql, sqlcmd, etc.

# 7. Full refresh
dvt run --full-refresh --target <target> --profiles-dir test_project/Connections --project-dir test_project

# 8. Verify DDL contract (tables were dropped and recreated)
```

## Cross-Engine Federation Test Models

These models test DVT's core differentiator: reading from one database engine and writing to another via Spark JDBC federation.

**Location**: `Testing_adapters_docker/test_project/models/federation/`

| Model | Source Engine | Target Engine | What It Tests |
|-------|-------------|--------------|---------------|
| `pg_to_mysql.sql` | PostgreSQL | MySQL | PG JDBC extraction + MySQL JDBC load |
| `pg_to_mssql.sql` | PostgreSQL | SQL Server | PG JDBC extraction + MSSQL JDBC load, type mapping |
| `mysql_to_pg.sql` | MySQL | PostgreSQL | MySQL JDBC extraction + PG JDBC load |
| `mssql_to_pg.sql` | SQL Server | PostgreSQL | MSSQL JDBC extraction + PG JDBC load |
| `cross_pg_mysql.sql` | PG + MySQL | PostgreSQL | Multi-source join in Spark SQL, dual JDBC extraction |
| `mariadb_to_pg.sql` | MariaDB | PostgreSQL | MariaDB JDBC extraction |

### Federation test data flow

```
Source DB              Spark (local)           Target DB
  [PG]  --JDBC--->  [Extract to staging]  --JDBC--->  [MySQL]
  [MySQL] --JDBC--> [Extract to staging]  --JDBC--->  [PG]
  [MSSQL] --JDBC--> [Extract to staging]  --JDBC--->  [PG]
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

# Run everything including federation
./run_adapter_tests.sh all

# Run specific engine + federation
./run_adapter_tests.sh postgres && ./run_adapter_tests.sh federation
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
