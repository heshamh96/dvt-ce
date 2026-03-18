"""
DVT Parse State — detects default target changes between parses.

Stores the last known default target (name, adapter type, hostname) in
.dvt/state.json. On each dvt parse/run/build, compares against the current
default target and warns if:

- DVT008: Adapter type changed (e.g., postgres → snowflake) — migrate model dialects
- DVT009: Same type but different hostname (e.g., switching Snowflake instances) — check sources
"""

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Maps adapter type → which credential field is the "hostname"
ADAPTER_HOSTNAME_FIELD: Dict[str, List[str]] = {
    "postgres": ["host"],
    "redshift": ["host"],
    "mysql": ["host", "server"],
    "mariadb": ["host", "server"],
    "sqlserver": ["host", "server"],
    "oracle": ["host"],
    "snowflake": ["account"],
    "bigquery": ["project"],
    "databricks": ["host"],
    "duckdb": ["path", "database"],
    "spark": ["host"],
    "clickhouse": ["host"],
    "trino": ["host"],
    "sqlite": ["path", "database"],
    "fabric": ["host", "server"],
}


def get_hostname(output_config: Dict[str, Any]) -> str:
    """Extract the hostname (including port) from a profiles.yml output config.

    For engines that use host+port (postgres, mysql, oracle, sqlserver),
    includes the port to distinguish instances on the same host.
    For Snowflake, uses account. For BigQuery, uses project.
    """
    adapter_type = output_config.get("type", "")
    fields = ADAPTER_HOSTNAME_FIELD.get(adapter_type, ["host"])
    host = ""
    for field in fields:
        val = output_config.get(field)
        if val:
            host = str(val)
            break
    if not host:
        return ""

    # Include port for engines that differentiate instances by port
    port = output_config.get("port")
    if port and adapter_type in (
        "postgres",
        "redshift",
        "mysql",
        "mariadb",
        "sqlserver",
        "oracle",
        "clickhouse",
        "trino",
    ):
        host = f"{host}:{port}"

    return host


def load_parse_state(project_dir: str) -> Optional[Dict[str, str]]:
    """Load the previous parse state from .dvt/state.json.

    Returns dict with keys: name, type, hostname. Or None if no state.
    """
    state_path = os.path.join(project_dir, ".dvt", "state.json")
    if not os.path.isfile(state_path):
        return None
    try:
        with open(state_path, "r") as f:
            return json.load(f)
    except Exception:
        return None


def save_parse_state(
    project_dir: str, target_name: str, adapter_type: str, hostname: str
) -> None:
    """Save the current default target info to .dvt/state.json."""
    state_dir = os.path.join(project_dir, ".dvt")
    os.makedirs(state_dir, exist_ok=True)
    state_path = os.path.join(state_dir, "state.json")
    state = {
        "name": target_name,
        "type": adapter_type,
        "hostname": hostname,
    }
    try:
        with open(state_path, "w") as f:
            json.dump(state, f, indent=2)
    except Exception:
        pass


def check_target_changes(
    project_dir: str,
    current_target: str,
    current_config: Dict[str, Any],
) -> List[str]:
    """Compare current default target against previous parse state.

    Returns list of warning messages. Empty = no changes detected.
    Saves the current state after checking.
    """
    warnings = []

    current_type = current_config.get("type", "")
    current_hostname = get_hostname(current_config)

    # Load previous state
    prev_state = load_parse_state(project_dir)

    if prev_state:
        prev_type = prev_state.get("type", "")
        prev_hostname = prev_state.get("hostname", "")
        prev_name = prev_state.get("name", "")

        if prev_type and current_type and prev_type != current_type:
            # Case A: Adapter type changed
            warnings.append(
                f"DVT008: Default target adapter type changed from '{prev_type}' "
                f"to '{current_type}'. You need to migrate all pushdown models "
                f"from {prev_type} dialect to {current_type} dialect."
            )
        elif (
            prev_type == current_type
            and prev_hostname
            and current_hostname
            and prev_hostname != current_hostname
        ):
            # Case B: Same type, different hostname (instance switch)
            warnings.append(
                f"DVT009: Default target hostname changed from '{prev_hostname}' "
                f"to '{current_hostname}' (same adapter type '{current_type}'). "
                f"If you have sources on the old instance, add connection: to "
                f"those source definitions pointing to the old target."
            )
        # Case C: Same type, same hostname = no warning (normal env switching)

    # Save current state for next parse
    save_parse_state(project_dir, current_target, current_type, current_hostname)

    return warnings
