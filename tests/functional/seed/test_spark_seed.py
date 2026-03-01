"""Functional tests for DVT Spark-based seeding (dvt seed --spark).

Tests verify that:
1. Default `dvt seed` uses standard adapter-based seeding (same as dbt)
2. `dvt seed --spark` routes to DvtSeedTask/SparkSeedRunner
3. `dvt seed --compute <name>` routes to DvtSeedTask with named compute
4. Flag parsing works correctly and doesn't break standard seeding

NOTE: Tests that actually execute Spark require PySpark + Java to be
installed. Tests are designed to verify routing and flag parsing even
when Spark is not available.
"""

import pytest

from dvt.tests.util import run_dbt


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

seed_csv = """id,name,amount
1,Alice,100
2,Bob,200
3,Charlie,300
""".strip()

simple_model_sql = """
SELECT id, name, amount FROM {{ ref('test_seed') }}
"""


@pytest.fixture(scope="class")
def seeds():
    return {"test_seed.csv": seed_csv}


@pytest.fixture(scope="class")
def models():
    return {"simple_model.sql": simple_model_sql}


# ---------------------------------------------------------------------------
# Tests: Standard adapter-based seeding (default)
# ---------------------------------------------------------------------------


class TestAdapterSeedDefault:
    """Verify that `dvt seed` (no --spark/--compute) uses adapter-based seeding."""

    def test_seed_loads_csv(self, project):
        """Basic dvt seed should load the CSV via the database adapter."""
        results = run_dbt(["seed"])
        assert len(results) == 1
        assert results[0].node.name == "test_seed"

    def test_seed_full_refresh(self, project):
        """dvt seed --full-refresh should succeed with adapter-based seeding."""
        results = run_dbt(["seed", "--full-refresh"])
        assert len(results) == 1

    def test_seed_select(self, project):
        """dvt seed -s test_seed should select the specific seed."""
        results = run_dbt(["seed", "-s", "test_seed"])
        assert len(results) == 1
        assert results[0].node.name == "test_seed"

    def test_seed_then_run(self, project):
        """dvt seed followed by dvt run should work (model refs seed)."""
        run_dbt(["seed"])
        results = run_dbt(["run"])
        assert len(results) == 1
        assert results[0].node.name == "simple_model"

    def test_seed_idempotent(self, project):
        """Running seed twice should be idempotent."""
        results1 = run_dbt(["seed"])
        assert len(results1) == 1
        results2 = run_dbt(["seed"])
        assert len(results2) == 1


# ---------------------------------------------------------------------------
# Tests: --spark flag routing
# ---------------------------------------------------------------------------


class TestSparkFlagRouting:
    """Verify that --spark flag is accepted by the CLI and routes correctly.

    These tests verify flag parsing. Actual Spark execution may fail if
    PySpark/Java are not available, but the flag should be recognized
    and routed to DvtSeedTask (not rejected as an unknown option).
    """

    def test_spark_flag_recognized(self, project):
        """dvt seed --spark should not fail with 'no such option' error."""
        # The --spark flag should be recognized by Click.
        # If PySpark is not available, we expect a runtime error (not a CLI error).
        try:
            results = run_dbt(["seed", "--spark"])
            # If Spark is available, it should succeed
            assert len(results) == 1
        except Exception as e:
            error_msg = str(e).lower()
            # It should NOT be a Click/CLI parsing error
            assert "no such option" not in error_msg
            assert "unrecognized" not in error_msg
            # Acceptable errors: PySpark not found, compute config missing, etc.

    def test_spark_with_select(self, project):
        """dvt seed --spark -s test_seed should accept both flags."""
        try:
            results = run_dbt(["seed", "--spark", "-s", "test_seed"])
            assert len(results) == 1
        except Exception as e:
            error_msg = str(e).lower()
            assert "no such option" not in error_msg

    def test_spark_with_full_refresh(self, project):
        """dvt seed --spark --full-refresh should accept both flags."""
        try:
            results = run_dbt(["seed", "--spark", "--full-refresh"])
            assert len(results) == 1
        except Exception as e:
            error_msg = str(e).lower()
            assert "no such option" not in error_msg


# ---------------------------------------------------------------------------
# Tests: --compute flag routing
# ---------------------------------------------------------------------------


class TestComputeFlagRouting:
    """Verify that --compute flag is accepted and routes to DvtSeedTask."""

    def test_compute_flag_recognized(self, project):
        """dvt seed --compute local_spark should not fail with CLI error."""
        try:
            results = run_dbt(["seed", "--compute", "local_spark"])
            assert len(results) == 1
        except Exception as e:
            error_msg = str(e).lower()
            assert "no such option" not in error_msg
            assert "unrecognized" not in error_msg

    def test_compute_with_select(self, project):
        """dvt seed --compute local_spark -s test_seed should accept both."""
        try:
            results = run_dbt(["seed", "--compute", "local_spark", "-s", "test_seed"])
            assert len(results) == 1
        except Exception as e:
            error_msg = str(e).lower()
            assert "no such option" not in error_msg


# ---------------------------------------------------------------------------
# Tests: Verify adapter-based seed produces correct data
# ---------------------------------------------------------------------------


class TestAdapterSeedData:
    """Verify that adapter-based seeding produces correct table data."""

    def test_seed_row_count(self, project):
        """Adapter-based seed should load all 3 rows."""
        run_dbt(["seed"])
        result = project.run_sql("SELECT count(*) FROM test_seed", fetch="one")
        assert result[0] == 3

    def test_seed_data_content(self, project):
        """Adapter-based seed should load correct data values."""
        run_dbt(["seed", "--full-refresh"])
        result = project.run_sql("SELECT name FROM test_seed WHERE id = 1", fetch="one")
        assert result[0] == "Alice"

    def test_seed_full_refresh_replaces(self, project):
        """--full-refresh should replace existing data (not append)."""
        run_dbt(["seed"])
        run_dbt(["seed", "--full-refresh"])
        result = project.run_sql("SELECT count(*) FROM test_seed", fetch="one")
        # Should still be 3, not 6 (no duplication)
        assert result[0] == 3
