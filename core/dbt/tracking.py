"""
DVT Usage Audit — anonymous usage tracking via PostHog.

Tracks command usage, adapter types, execution paths, error rates,
and other anonymous metrics to help improve DVT. All data is anonymized
(hashed model names, no SQL/credentials). Users can opt out via:

  --no-send-anonymous-usage-stats
  DVT_SEND_ANONYMOUS_USAGE_STATS=false
"""

import hashlib
import os
import platform
import traceback
import uuid
from contextlib import contextmanager
from datetime import datetime
from typing import Any, Dict, Optional

import pytz

from dbt import version as dbt_version
from dbt.adapters.exceptions import FailedToConnectError
from dbt.clients.yaml_helper import safe_load, yaml  # noqa:F401
from dbt.events.types import (
    DisableTracking,
    FlushEvents,
    FlushEventsFailure,
    MainEncounteredError,
    SendEventFailure,
    SendingEvent,
    TrackingInitializeFailure,
)
from dbt_common.events.base_types import EventMsg
from dbt_common.events.functions import fire_event, get_invocation_id, msg_to_dict
from dbt_common.exceptions import NotImplementedError

# ---------------------------------------------------------------------------
# PostHog configuration
# ---------------------------------------------------------------------------
POSTHOG_API_KEY = "phc_B9D5fqEjUY6qkApj8ekAc5FnAxVudwT7bvVYzTnxb7yb"
POSTHOG_HOST = "https://us.i.posthog.com"

DBT_INVOCATION_ENV = "DBT_INVOCATION_ENV"

# Initialize PostHog (lazy — disabled until initialize_from_flags enables it)
_posthog = None


def _get_posthog():
    """Lazy-load PostHog to avoid import errors if package is missing."""
    global _posthog
    if _posthog is None:
        try:
            import posthog

            posthog.project_api_key = POSTHOG_API_KEY
            posthog.host = POSTHOG_HOST
            posthog.disabled = True  # Disabled until tracking is enabled
            _posthog = posthog
        except ImportError:
            pass
    return _posthog


# ---------------------------------------------------------------------------
# User identification
# ---------------------------------------------------------------------------


class User:
    def __init__(self, cookie_dir) -> None:
        self.do_not_track = True
        self.cookie_dir = cookie_dir
        self.id = None
        self.invocation_id = get_invocation_id()
        self.run_started_at = datetime.now(tz=pytz.utc)

    def state(self):
        return "do not track" if self.do_not_track else "tracking"

    @property
    def cookie_path(self):
        return os.path.join(self.cookie_dir, ".user.yml")

    def initialize(self):
        self.do_not_track = False
        cookie = self.get_cookie()
        self.id = cookie.get("id")

    def disable_tracking(self):
        self.do_not_track = True
        self.id = None
        self.cookie_dir = None
        ph = _get_posthog()
        if ph:
            ph.disabled = True

    def set_cookie(self):
        user = {"id": str(uuid.uuid4())}
        cookie_path = os.path.abspath(self.cookie_dir)
        profiles_file = os.path.join(cookie_path, "profiles.yml")
        if os.path.exists(cookie_path) and os.path.exists(profiles_file):
            with open(self.cookie_path, "w") as fh:
                yaml.dump(user, fh)
        return user

    def get_cookie(self):
        if not os.path.isfile(self.cookie_path):
            user = self.set_cookie()
        else:
            with open(self.cookie_path, "r") as fh:
                try:
                    user = safe_load(fh)
                    if user is None:
                        user = self.set_cookie()
                except yaml.reader.ReaderError:
                    user = self.set_cookie()
        return user


active_user: Optional[User] = None


# ---------------------------------------------------------------------------
# Base properties (included with every event)
# ---------------------------------------------------------------------------


def _base_properties() -> Dict[str, Any]:
    """Properties included with every event."""
    return {
        "invocation_id": get_invocation_id(),
        "edition": os.environ.get("DVT_EDITION", "ce"),
        "dvt_version": str(dbt_version.installed),
        "os": platform.platform(),
        "python_version": platform.python_version(),
        "environment": os.getenv(DBT_INVOCATION_ENV, "manual"),
    }


# ---------------------------------------------------------------------------
# Core tracking function
# ---------------------------------------------------------------------------


def track(user, event_name: str, properties: Optional[Dict[str, Any]] = None):
    """Send an event to PostHog."""
    if user is None or user.do_not_track:
        return

    ph = _get_posthog()
    if ph is None or ph.disabled:
        return

    props = _base_properties()
    if properties:
        props.update(properties)

    fire_event(SendingEvent(kwargs=str(props)))
    try:
        ph.capture(user.id, event_name, props)
    except Exception:
        fire_event(SendEventFailure())


# ---------------------------------------------------------------------------
# dbt-inherited tracking functions (same signatures, PostHog backend)
# ---------------------------------------------------------------------------


def track_invocation_start(invocation_context):
    data = {"progress": "start", "result_type": None}
    data.update(invocation_context)
    track(active_user, "invocation_start", data)


def track_invocation_end(invocation_context, result_type=None):
    data = {"progress": "end", "result_type": result_type}
    data.update(invocation_context)
    track(active_user, "invocation_end", data)


def track_invalid_invocation(args=None, result_type=None):
    assert active_user is not None
    context = get_base_invocation_context()
    if args:
        context.update({"command": args.which})
    track(active_user, "invocation_invalid", context)


def track_project_id(options):
    assert active_user is not None
    track(active_user, "project_id", options)


def track_adapter_info(options):
    assert active_user is not None
    track(active_user, "adapter_info", options)


def track_project_load(options):
    assert active_user is not None
    track(active_user, "load_project", options)


def track_resource_counts(resource_counts):
    assert active_user is not None
    track(active_user, "resource_counts", resource_counts)


def track_model_run(options):
    assert active_user is not None
    track(active_user, "run_model", options)


def track_rpc_request(options):
    assert active_user is not None
    track(active_user, "rpc_request", options)


def track_package_install(command_name: str, project_hashed_name: Optional[str], options):
    assert active_user is not None
    data = {"command": command_name, "project_id": project_hashed_name}
    data.update(options)
    track(active_user, "package_install", data)


def track_deprecation_warn(options):
    assert active_user is not None
    track(active_user, "deprecation_warn", options)


def track_behavior_change_warn(msg: EventMsg) -> None:
    if msg.info.name != "BehaviorChangeEvent" or active_user is None:
        return
    track(active_user, "behavior_change_warn", msg_to_dict(msg))


def track_experimental_parser_sample(options):
    assert active_user is not None
    track(active_user, "experimental_parser", options)


def track_partial_parser(options):
    assert active_user is not None
    track(active_user, "partial_parser", options)


def track_plugin_get_nodes(options):
    assert active_user is not None
    track(active_user, "plugin_get_nodes", options)


def track_runnable_timing(options):
    assert active_user is not None
    track(active_user, "runnable_timing", options)


# ---------------------------------------------------------------------------
# DVT-specific tracking functions
# ---------------------------------------------------------------------------


def track_dvt_extraction(properties: Dict[str, Any]):
    """Track extraction path model execution (Sling → DuckDB → Sling)."""
    if active_user is not None:
        track(active_user, "dvt_extraction", properties)


def track_dvt_seed(properties: Dict[str, Any]):
    """Track seed loaded via Sling."""
    if active_user is not None:
        track(active_user, "dvt_seed", properties)


def track_dvt_sync(properties: Dict[str, Any]):
    """Track environment bootstrap run."""
    if active_user is not None:
        track(active_user, "dvt_sync", properties)


def track_dvt_debug(properties: Dict[str, Any]):
    """Track connection test results."""
    if active_user is not None:
        track(active_user, "dvt_debug", properties)


def track_dvt_retract(properties: Dict[str, Any]):
    """Track models dropped from targets."""
    if active_user is not None:
        track(active_user, "dvt_retract", properties)


def track_dvt_docs(properties: Dict[str, Any]):
    """Track docs generation with cross-engine catalog."""
    if active_user is not None:
        track(active_user, "dvt_docs", properties)


def track_dvt_init(properties: Dict[str, Any]):
    """Track project initialization."""
    if active_user is not None:
        track(active_user, "dvt_init", properties)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def get_base_invocation_context():
    assert active_user is not None
    return {
        "project_id": None,
        "user_id": active_user.id,
        "invocation_id": active_user.invocation_id,
        "command": None,
        "options": None,
        "version": str(dbt_version.installed),
        "run_type": "regular",
        "adapter_type": None,
        "adapter_unique_id": None,
    }


def flush():
    fire_event(FlushEvents())
    try:
        ph = _get_posthog()
        if ph:
            ph.flush()
    except Exception:
        fire_event(FlushEventsFailure())


def disable_tracking():
    global active_user
    if active_user is not None:
        active_user.disable_tracking()
    else:
        active_user = User(None)


def do_not_track():
    global active_user
    active_user = User(None)


def initialize_from_flags(send_anonymous_usage_stats, profiles_dir):
    global active_user
    if send_anonymous_usage_stats:
        active_user = User(profiles_dir)
        try:
            active_user.initialize()
            ph = _get_posthog()
            if ph:
                ph.disabled = False
        except Exception:
            fire_event(TrackingInitializeFailure(exc_info=traceback.format_exc()))
            active_user = User(None)
    else:
        active_user = User(None)


@contextmanager
def track_run(run_command=None):
    invocation_context = get_base_invocation_context()
    invocation_context["command"] = run_command

    track_invocation_start(invocation_context)
    try:
        yield
        track_invocation_end(invocation_context, result_type="ok")
    except (NotImplementedError, FailedToConnectError) as e:
        fire_event(MainEncounteredError(exc=str(e)))
        track_invocation_end(invocation_context, result_type="error")
    except Exception:
        track_invocation_end(invocation_context, result_type="error")
        raise
    finally:
        flush()
