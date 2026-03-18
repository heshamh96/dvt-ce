"""
Read source connection mappings directly from sources.yml files.

dbt's source parser doesn't preserve the `connection:` property on sources
(it's a DVT extension, not a standard dbt field). DVT reads sources.yml
files directly to build a mapping of source_name → connection_name.

This mapping is used by the target resolver to detect which sources are
remote (connection != model target) and need Sling extraction.
"""

import logging
import os
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


# Mapping: (source_name, table_name) → connection_name
SourceConnectionMap = Dict[str, str]  # source_name → connection_name


def load_source_connections(project_dir: str) -> SourceConnectionMap:
    """Scan the project for sources.yml / schema.yml files and extract connection mappings.

    Searches all YAML files under model-paths for source definitions with
    a `connection:` property.

    Args:
        project_dir: Root directory of the dbt project.

    Returns:
        Dict mapping source_name → connection_name.
    """
    connections: SourceConnectionMap = {}

    # Read dbt_project.yml to find model-paths
    project_yml = os.path.join(project_dir, "dbt_project.yml")
    model_paths = ["models"]  # default
    if os.path.isfile(project_yml):
        try:
            with open(project_yml, "r") as f:
                project_config = yaml.safe_load(f) or {}
            model_paths = project_config.get("model-paths", model_paths)
        except Exception:
            pass

    # Scan all YAML files under model-paths
    for model_path in model_paths:
        full_path = os.path.join(project_dir, model_path)
        if not os.path.isdir(full_path):
            continue

        for root, dirs, files in os.walk(full_path):
            for filename in files:
                if not filename.endswith((".yml", ".yaml")):
                    continue
                filepath = os.path.join(root, filename)
                _extract_connections_from_file(filepath, connections)

    if connections:
        logger.info(
            f"DVT: Found {len(connections)} source connections: "
            f"{', '.join(f'{k}={v}' for k, v in sorted(connections.items()))}"
        )

    return connections


def _extract_connections_from_file(
    filepath: str, connections: SourceConnectionMap
) -> None:
    """Extract source connection mappings from a single YAML file."""
    try:
        with open(filepath, "r") as f:
            content = yaml.safe_load(f)
    except Exception:
        return

    if not isinstance(content, dict):
        return

    sources = content.get("sources", [])
    if not isinstance(sources, list):
        return

    for source in sources:
        if not isinstance(source, dict):
            continue

        source_name = source.get("name")
        if not source_name:
            continue

        # DVT reads connection from top-level (DVT extension)
        connection = source.get("connection")

        # Fallback: meta.connection
        if not connection:
            meta = source.get("meta", {})
            if isinstance(meta, dict):
                connection = meta.get("connection")

        # Fallback: config.connection
        if not connection:
            config = source.get("config", {})
            if isinstance(config, dict):
                connection = config.get("connection")

        if connection:
            connections[source_name] = connection


def validate_source_connections(
    source_connections: SourceConnectionMap,
    profiles: Dict[str, Any],
    default_target: str,
) -> List[str]:
    """Validate source connections against the default target.

    Returns a list of error messages. Empty list = all valid.

    DVT113: Source has connection: to same adapter type as default target.
    """
    errors = []

    # Get default target's adapter type
    default_type = None
    for profile_name, profile_data in profiles.items():
        if not isinstance(profile_data, dict):
            continue
        outputs = profile_data.get("outputs", {})
        if default_target in outputs:
            default_type = outputs[default_target].get("type", "")
            break

    if not default_type:
        return errors

    # Check each source connection
    for source_name, connection_name in source_connections.items():
        # Find the connection's adapter type
        conn_type = None
        for profile_name, profile_data in profiles.items():
            if not isinstance(profile_data, dict):
                continue
            outputs = profile_data.get("outputs", {})
            if connection_name in outputs:
                conn_type = outputs[connection_name].get("type", "")
                break

        if conn_type and conn_type == default_type:
            errors.append(
                f"DVT113: Source '{source_name}' has connection '{connection_name}' "
                f"which uses the same adapter type '{conn_type}' as the default target. "
                f"Remove the connection: property — sources on the default adapter type "
                f"follow --target automatically."
            )

    return errors


def get_source_connection(
    source_name: str,
    source_connections: SourceConnectionMap,
) -> Optional[str]:
    """Look up the connection for a source by name.

    Args:
        source_name: The source name (e.g., 'crm', 'pg_source').
        source_connections: The connection map from load_source_connections().

    Returns:
        The connection name (profiles.yml output name) or None.
    """
    return source_connections.get(source_name)
