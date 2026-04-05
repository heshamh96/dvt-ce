"""Tests for DVT retract — dialect-aware DROP SQL generation."""

import pytest

from dvt.tasks.retract import _build_drop_sql


class TestBuildDropSql:
    """Test that each engine generates correct DROP SQL."""

    def test_postgres_table_cascade(self):
        sql = _build_drop_sql("postgres", "public.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS public.my_table CASCADE"

    def test_postgres_view_cascade(self):
        sql = _build_drop_sql("postgres", "public.my_view", "VIEW")
        assert sql == "DROP VIEW IF EXISTS public.my_view CASCADE"

    def test_redshift_table_cascade(self):
        sql = _build_drop_sql("redshift", "public.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS public.my_table CASCADE"

    def test_snowflake_table_cascade(self):
        sql = _build_drop_sql("snowflake", "PUBLIC.MY_TABLE", "TABLE")
        assert sql == "DROP TABLE IF EXISTS PUBLIC.MY_TABLE CASCADE"

    def test_duckdb_table_cascade(self):
        sql = _build_drop_sql("duckdb", "main.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS main.my_table CASCADE"

    def test_oracle_table_plsql(self):
        sql = _build_drop_sql("oracle", "SYSTEM.MY_TABLE", "TABLE")
        assert "CASCADE CONSTRAINTS PURGE" in sql
        assert "EXECUTE IMMEDIATE" in sql
        assert "SQLCODE != -942" in sql
        assert "BEGIN" in sql
        assert "END;" in sql

    def test_oracle_view_plsql(self):
        sql = _build_drop_sql("oracle", "SYSTEM.MY_VIEW", "VIEW")
        assert "CASCADE CONSTRAINTS" in sql
        assert "PURGE" not in sql  # PURGE only for tables
        assert "EXECUTE IMMEDIATE" in sql
        assert "SQLCODE != -942" in sql

    def test_oracle_no_if_exists(self):
        """Oracle handles IF EXISTS via exception, not SQL syntax."""
        sql = _build_drop_sql("oracle", "SYSTEM.MY_TABLE", "TABLE")
        assert "IF EXISTS" not in sql

    def test_mysql_no_cascade(self):
        sql = _build_drop_sql("mysql", "mydb.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS mydb.my_table"
        assert "CASCADE" not in sql

    def test_mariadb_no_cascade(self):
        sql = _build_drop_sql("mariadb", "mydb.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS mydb.my_table"

    def test_sqlserver_no_cascade(self):
        sql = _build_drop_sql("sqlserver", "dbo.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS dbo.my_table"

    def test_databricks_no_cascade(self):
        sql = _build_drop_sql("databricks", "default.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS default.my_table"

    def test_bigquery_no_cascade(self):
        sql = _build_drop_sql("bigquery", "dataset.my_table", "TABLE")
        assert sql == "DROP TABLE IF EXISTS dataset.my_table"

    def test_all_engines_produce_valid_sql(self):
        """Every supported engine should produce non-empty SQL."""
        engines = [
            "postgres", "redshift", "snowflake", "duckdb",
            "oracle", "mysql", "mariadb", "sqlserver",
            "databricks", "bigquery",
        ]
        for engine in engines:
            sql = _build_drop_sql(engine, "schema.table", "TABLE")
            assert sql, f"{engine} produced empty SQL"
            assert "schema.table" in sql, f"{engine} missing table name"
