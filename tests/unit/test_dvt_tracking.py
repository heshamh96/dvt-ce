"""Tests for DVT-specific PostHog tracking functions."""

import pytest

from dbt.tracking import (
    initialize_from_flags,
    disable_tracking,
    track_dvt_extraction,
    track_dvt_seed,
    track_dvt_sync,
    track_dvt_debug,
    track_dvt_retract,
    track_dvt_docs,
    track_dvt_init,
    track,
    _base_properties,
    active_user,
)


@pytest.fixture
def tracking_enabled(mocker):
    """Initialize tracking with a mock user and mock PostHog."""
    initialize_from_flags(True, "")
    mocker.patch("dbt.tracking.User.set_cookie").return_value = {"id": "test-uuid"}

    ph = mocker.MagicMock()
    ph.disabled = False
    mocker.patch("dbt.tracking._get_posthog", return_value=ph)

    yield ph

    disable_tracking()


@pytest.fixture
def tracking_disabled(mocker):
    """Initialize tracking in disabled state."""
    initialize_from_flags(False, "")
    ph = mocker.MagicMock()
    ph.disabled = True
    mocker.patch("dbt.tracking._get_posthog", return_value=ph)

    yield ph

    disable_tracking()


class TestBaseProperties:
    def test_base_properties_contains_required_fields(self):
        props = _base_properties()
        assert "invocation_id" in props
        assert "edition" in props
        assert "dvt_version" in props
        assert "os" in props
        assert "python_version" in props
        assert "environment" in props

    def test_edition_defaults_to_ce(self):
        props = _base_properties()
        assert props["edition"] == "ce"


class TestDvtExtractionTracking:
    def test_tracks_extraction_with_adapter_types(self, tracking_enabled):
        track_dvt_extraction({
            "source_adapter_types": ["postgres", "snowflake"],
            "target_adapter_type": "databricks",
            "execution_path": "extraction",
            "row_count": 100,
            "duration_seconds": 5.2,
            "success": True,
        })
        tracking_enabled.capture.assert_called_once()
        call_args = tracking_enabled.capture.call_args
        props = call_args[0][2]  # third positional arg = properties
        assert props["source_adapter_types"] == ["postgres", "snowflake"]
        assert props["target_adapter_type"] == "databricks"
        assert props["row_count"] == 100
        assert props["success"] is True

    def test_does_not_track_when_disabled(self, tracking_disabled):
        track_dvt_extraction({"success": True})
        tracking_disabled.capture.assert_not_called()


class TestDvtSeedTracking:
    def test_tracks_seed_with_adapter_type(self, tracking_enabled):
        track_dvt_seed({
            "target_adapter_type": "postgres",
            "row_count": 500,
            "duration_seconds": 1.5,
            "success": True,
        })
        tracking_enabled.capture.assert_called_once()
        call_args = tracking_enabled.capture.call_args
        props = call_args[0][2]
        assert props["target_adapter_type"] == "postgres"
        assert props["row_count"] == 500

    def test_does_not_track_when_disabled(self, tracking_disabled):
        track_dvt_seed({"success": True})
        tracking_disabled.capture.assert_not_called()


class TestDvtSyncTracking:
    def test_tracks_sync_with_adapter_types(self, tracking_enabled):
        track_dvt_sync({
            "adapter_types": ["postgres", "mysql", "snowflake"],
            "success": True,
        })
        tracking_enabled.capture.assert_called_once()
        call_args = tracking_enabled.capture.call_args
        props = call_args[0][2]
        assert "postgres" in props["adapter_types"]
        assert "snowflake" in props["adapter_types"]


class TestDvtDebugTracking:
    def test_tracks_debug_with_connection_results(self, tracking_enabled):
        track_dvt_debug({
            "connection_count": 5,
            "ok_count": 4,
            "fail_count": 1,
            "adapter_types": ["postgres", "mysql", "snowflake", "oracle", "sqlserver"],
            "success": False,
        })
        tracking_enabled.capture.assert_called_once()
        call_args = tracking_enabled.capture.call_args
        props = call_args[0][2]
        assert len(props["adapter_types"]) == 5
        assert props["success"] is False


class TestDvtRetractTracking:
    def test_tracks_retract(self, tracking_enabled):
        track_dvt_retract({
            "model_count": 10,
            "ok_count": 8,
            "error_count": 2,
            "duration_seconds": 3.5,
            "success": False,
        })
        tracking_enabled.capture.assert_called_once()


class TestDvtDocsTracking:
    def test_tracks_docs_generate(self, tracking_enabled):
        track_dvt_docs({"success": True})
        tracking_enabled.capture.assert_called_once()
        call_args = tracking_enabled.capture.call_args
        assert call_args[0][1] == "dvt_docs"


class TestDvtInitTracking:
    def test_tracks_init(self, tracking_enabled):
        track_dvt_init({"project_name_hash": "abc123"})
        tracking_enabled.capture.assert_called_once()
        call_args = tracking_enabled.capture.call_args
        assert call_args[0][1] == "dvt_init"


class TestTrackingDisabled:
    def test_all_dvt_events_silent_when_disabled(self, tracking_disabled):
        """No DVT tracking function should call PostHog when tracking is disabled."""
        track_dvt_extraction({"success": True})
        track_dvt_seed({"success": True})
        track_dvt_sync({"success": True})
        track_dvt_debug({"success": True})
        track_dvt_retract({"success": True})
        track_dvt_docs({"success": True})
        track_dvt_init({"success": True})
        tracking_disabled.capture.assert_not_called()


class TestTrackingEdition:
    def test_ce_edition_in_events(self, tracking_enabled):
        track_dvt_extraction({"success": True})
        call_args = tracking_enabled.capture.call_args
        props = call_args[0][2]
        assert props["edition"] == "ce"
