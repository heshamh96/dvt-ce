import pytest

from dbt.tracking import (
    disable_tracking,
    initialize_from_flags,
    track_behavior_change_warn,
)
from dbt_common.behavior_flags import Behavior
from dbt_common.events.event_manager_client import (
    add_callback_to_manager,
    cleanup_event_logger,
)


@pytest.fixture
def posthog_capture(mocker):
    # initialize `active_user` without writing the cookie to disk
    initialize_from_flags(True, "")
    mocker.patch("dbt.tracking.User.set_cookie").return_value = {"id": 42}

    # add the relevant callback to the event manager
    add_callback_to_manager(track_behavior_change_warn)

    # Mock the PostHog capture call — this is what actually sends events.
    # When tracking is disabled, track() returns early and capture is never called.
    capture_mock = mocker.patch("dbt.tracking._get_posthog")
    ph = mocker.MagicMock()
    ph.disabled = False
    capture_mock.return_value = ph

    yield ph

    # teardown
    cleanup_event_logger()
    disable_tracking()


def test_false_evaluation_triggers_tracking(posthog_capture):
    behavior = Behavior(
        [{"name": "my_flag", "default": False, "description": "This is a false flag."}], {}
    )
    if behavior.my_flag:
        assert False, "This flag should evaluate to false and skip this line"
    assert posthog_capture.capture.called


def test_true_evaluation_does_not_trigger_tracking(posthog_capture):
    behavior = Behavior(
        [{"name": "my_flag", "default": True, "description": "This is a true flag."}], {}
    )
    if behavior.my_flag:
        pass
    else:
        assert False, "This flag should evaluate to true and skip this line"
    assert not posthog_capture.capture.called


def test_false_evaluation_does_not_trigger_tracking_when_disabled(posthog_capture):
    disable_tracking()

    behavior = Behavior(
        [{"name": "my_flag", "default": False, "description": "This is a false flag."}], {}
    )
    if behavior.my_flag:
        assert False, "This flag should evaluate to false and skip this line"
    # When tracking is disabled, track() returns early — PostHog capture never called
    assert not posthog_capture.capture.called
