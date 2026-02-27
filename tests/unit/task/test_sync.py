"""Unit tests for dvt sync task and JDBC integration (core/dvt/dvt_tasks/dvt_sync)."""

from pathlib import Path
from unittest import mock

import pytest
import yaml

from dvt.cli.flags import Flags
from dvt.dvt_tasks.dvt_sync import (
    DEFAULT_PYSPARK_VERSION,
    DvtSyncTask,
    _get_active_pyspark_version,
)


@pytest.fixture
def mock_flags(tmp_path):
    """Flags with project_dir and profiles_dir set to temp paths."""
    flags = mock.Mock(spec=Flags)
    flags.project_dir = str(tmp_path / "project")
    flags.PROFILES_DIR = str(tmp_path / "profiles")
    flags.PROJECT_DIR = str(tmp_path / "project")
    return flags


class TestDvtSyncTaskJdbcIntegration:
    """Sync task calls JDBC driver resolution and download for profile adapters."""

    @mock.patch("dvt.dvt_tasks.dvt_sync.download_jdbc_jars")
    @mock.patch("dvt.dvt_tasks.dvt_sync.get_jdbc_drivers_for_adapters")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_active_pyspark_version", return_value=None)
    @mock.patch("dvt.dvt_tasks.dvt_sync._run_uv_pip", return_value=True)
    @mock.patch("dvt.dvt_tasks.dvt_sync._run_pip", return_value=True)
    @mock.patch("dvt.dvt_tasks.dvt_sync._detect_package_manager", return_value="pip")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_require_adapters", return_value={})
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_adapter_types_from_profile")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_profile_name")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_env_python")
    @mock.patch("dvt.dvt_tasks.dvt_sync._find_project_env")
    @mock.patch("dvt.dvt_tasks.dvt_sync.get_nearest_project_dir")
    def test_sync_calls_jdbc_download_for_profile_adapters(
        self,
        mock_get_project_dir,
        mock_find_env,
        mock_get_env_python,
        mock_get_profile_name,
        mock_get_adapter_types,
        mock_detect_pm,
        mock_run_pip,
        mock_run_uv_pip,
        mock_get_require_adapters,
        mock_get_pyspark_version,
        mock_get_jdbc_drivers,
        mock_download_jdbc_jars,
        mock_flags,
        tmp_path,
    ):
        project_root = tmp_path / "project"
        project_root.mkdir()
        env_path = tmp_path / "venv"
        env_path.mkdir()
        (env_path / "bin").mkdir()
        (env_path / "bin" / "python").write_text("")
        (env_path / "bin" / "python").chmod(0o755)

        mock_get_project_dir.return_value = project_root
        mock_find_env.return_value = env_path
        mock_get_env_python.return_value = env_path / "bin" / "python"
        mock_get_profile_name.return_value = "my_profile"
        mock_get_adapter_types.return_value = ["postgres", "snowflake"]
        mock_get_jdbc_drivers.return_value = [
            ("org.postgresql", "postgresql", "42.7.3"),
            ("net.snowflake", "snowflake-jdbc", "3.10.3"),
        ]

        # Ensure profiles_dir exists for get_jdbc_drivers_dir
        profiles_dir = Path(mock_flags.PROFILES_DIR)
        profiles_dir.mkdir(parents=True, exist_ok=True)

        task = DvtSyncTask(mock_flags)
        task.run()

        mock_get_jdbc_drivers.assert_called_once_with(["postgres", "snowflake"])
        mock_download_jdbc_jars.assert_called_once()
        call_args = mock_download_jdbc_jars.call_args
        assert call_args[0][0] == [
            ("org.postgresql", "postgresql", "42.7.3"),
            ("net.snowflake", "snowflake-jdbc", "3.10.3"),
        ]
        # JDBC jars always go to canonical DVT home ~/.dvt/.jdbc_jars
        from dvt.config.user_config import get_jdbc_drivers_dir

        assert call_args[0][1] == get_jdbc_drivers_dir(None)
        assert call_args[1]["on_event"] is not None

    @mock.patch("dvt.dvt_tasks.dvt_sync.download_jdbc_jars")
    @mock.patch("dvt.dvt_tasks.dvt_sync.get_jdbc_drivers_for_adapters", return_value=[])
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_active_pyspark_version", return_value=None)
    @mock.patch("dvt.dvt_tasks.dvt_sync._run_uv_pip", return_value=True)
    @mock.patch("dvt.dvt_tasks.dvt_sync._run_pip", return_value=True)
    @mock.patch("dvt.dvt_tasks.dvt_sync._detect_package_manager", return_value="pip")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_require_adapters", return_value={})
    @mock.patch(
        "dvt.dvt_tasks.dvt_sync._get_adapter_types_from_profile", return_value=["spark"]
    )
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_profile_name")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_env_python")
    @mock.patch("dvt.dvt_tasks.dvt_sync._find_project_env")
    @mock.patch("dvt.dvt_tasks.dvt_sync.get_nearest_project_dir")
    def test_sync_does_not_call_download_when_no_jdbc_drivers_for_adapters(
        self,
        mock_get_project_dir,
        mock_find_env,
        mock_get_env_python,
        mock_get_profile_name,
        mock_get_adapter_types,
        mock_get_require_adapters,
        mock_detect_pm,
        mock_run_pip,
        mock_run_uv_pip,
        mock_get_pyspark_version,
        mock_get_jdbc_drivers,
        mock_download_jdbc_jars,
        mock_flags,
        tmp_path,
    ):
        project_root = tmp_path / "project"
        project_root.mkdir()
        env_path = tmp_path / "venv"
        env_path.mkdir()
        (env_path / "bin").mkdir()
        (env_path / "bin" / "python").write_text("")
        (env_path / "bin" / "python").chmod(0o755)

        mock_get_project_dir.return_value = project_root
        mock_find_env.return_value = env_path
        mock_get_env_python.return_value = env_path / "bin" / "python"
        mock_get_profile_name.return_value = "my_profile"
        Path(mock_flags.PROFILES_DIR).mkdir(parents=True, exist_ok=True)

        task = DvtSyncTask(mock_flags)
        task.run()

        mock_get_jdbc_drivers.assert_called_once_with(["spark"])
        mock_download_jdbc_jars.assert_not_called()

    @mock.patch("dvt.dvt_tasks.dvt_sync.download_jdbc_jars")
    @mock.patch("dvt.dvt_tasks.dvt_sync.get_jdbc_drivers_for_adapters", return_value=[])
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_active_pyspark_version", return_value=None)
    @mock.patch("dvt.dvt_tasks.dvt_sync._run_uv_pip", return_value=True)
    @mock.patch("dvt.dvt_tasks.dvt_sync._run_pip", return_value=True)
    @mock.patch("dvt.dvt_tasks.dvt_sync._detect_package_manager", return_value="pip")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_require_adapters", return_value={})
    @mock.patch(
        "dvt.dvt_tasks.dvt_sync._get_adapter_types_from_profile", return_value=[]
    )
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_profile_name")
    @mock.patch("dvt.dvt_tasks.dvt_sync._get_env_python")
    @mock.patch("dvt.dvt_tasks.dvt_sync._find_project_env")
    @mock.patch("dvt.dvt_tasks.dvt_sync.get_nearest_project_dir")
    def test_sync_uses_python_env_flag_and_skips_in_project_lookup(
        self,
        mock_get_project_dir,
        mock_find_env,
        mock_get_env_python,
        mock_get_profile_name,
        mock_get_adapter_types,
        mock_run_pip,
        mock_run_uv_pip,
        mock_get_require_adapters,
        mock_get_pyspark_version,
        mock_get_jdbc_drivers,
        mock_download_jdbc_jars,
        mock_flags,
        tmp_path,
    ):
        """When --python-env is set, sync uses it and does not look for .venv inside project."""
        project_root = tmp_path / "project"
        project_root.mkdir()
        explicit_venv = tmp_path / "external_venv"
        explicit_venv.mkdir()
        (explicit_venv / "bin").mkdir()
        (explicit_venv / "bin" / "python").write_text("")
        (explicit_venv / "bin" / "python").chmod(0o755)

        mock_get_project_dir.return_value = project_root
        mock_get_env_python.return_value = explicit_venv / "bin" / "python"
        mock_get_profile_name.return_value = "my_profile"
        Path(mock_flags.PROFILES_DIR).mkdir(parents=True, exist_ok=True)
        mock_flags.PYTHON_ENV = str(explicit_venv)

        task = DvtSyncTask(mock_flags)
        task.run()

        mock_find_env.assert_not_called()
        assert task.env_path == explicit_venv


class TestGetActivePysparkVersion:
    """Tests for _get_active_pyspark_version default behavior."""

    def _write_computes(self, path: Path, data: dict) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.dump(data))

    def test_returns_none_when_computes_file_missing(self, tmp_path):
        """No computes.yml at all → None (skip pyspark)."""
        result = _get_active_pyspark_version(tmp_path / "missing.yml", "my_profile")
        assert result is None

    def test_returns_none_when_profile_not_in_computes(self, tmp_path):
        """computes.yml exists but has a different profile → None."""
        computes_path = tmp_path / "computes.yml"
        self._write_computes(computes_path, {"other_profile": {"target": "dev"}})
        result = _get_active_pyspark_version(computes_path, "my_profile")
        assert result is None

    def test_returns_default_when_version_missing_from_active_compute(self, tmp_path):
        """Profile and target exist but version key is absent → default."""
        computes_path = tmp_path / "computes.yml"
        self._write_computes(
            computes_path,
            {
                "my_profile": {
                    "target": "dev",
                    "computes": {"dev": {"type": "spark", "master": "local[*]"}},
                }
            },
        )
        result = _get_active_pyspark_version(computes_path, "my_profile")
        assert result == DEFAULT_PYSPARK_VERSION

    def test_returns_default_when_version_is_empty_string(self, tmp_path):
        """Version key present but empty string → default."""
        computes_path = tmp_path / "computes.yml"
        self._write_computes(
            computes_path,
            {
                "my_profile": {
                    "target": "dev",
                    "computes": {
                        "dev": {"type": "spark", "version": "", "master": "local[*]"}
                    },
                }
            },
        )
        result = _get_active_pyspark_version(computes_path, "my_profile")
        assert result == DEFAULT_PYSPARK_VERSION

    def test_returns_default_when_no_computes_section(self, tmp_path):
        """Profile block exists but has no 'computes' key → default."""
        computes_path = tmp_path / "computes.yml"
        self._write_computes(computes_path, {"my_profile": {"target": "dev"}})
        result = _get_active_pyspark_version(computes_path, "my_profile")
        assert result == DEFAULT_PYSPARK_VERSION

    def test_returns_default_when_target_not_in_computes(self, tmp_path):
        """Profile block exists, computes section exists, but active target not found → default."""
        computes_path = tmp_path / "computes.yml"
        self._write_computes(
            computes_path,
            {
                "my_profile": {
                    "target": "prod",
                    "computes": {"dev": {"type": "spark", "version": "4.0.0"}},
                }
            },
        )
        result = _get_active_pyspark_version(computes_path, "my_profile")
        assert result == DEFAULT_PYSPARK_VERSION

    def test_returns_explicit_version_when_set(self, tmp_path):
        """Version explicitly set → use that version, not the default."""
        computes_path = tmp_path / "computes.yml"
        self._write_computes(
            computes_path,
            {
                "my_profile": {
                    "target": "dev",
                    "computes": {
                        "dev": {
                            "type": "spark",
                            "version": "4.0.0",
                            "master": "local[*]",
                        }
                    },
                }
            },
        )
        result = _get_active_pyspark_version(computes_path, "my_profile")
        assert result == "4.0.0"

    def test_default_pyspark_version_is_3_5_8(self):
        """Verify the constant itself so a bump is intentional."""
        assert DEFAULT_PYSPARK_VERSION == "3.5.8"
