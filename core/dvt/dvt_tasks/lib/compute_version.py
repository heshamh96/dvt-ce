# Copyright 2025 Hesham Badawi
# Licensed under the Apache License, Version 2.0
#
# DVT-specific compute engine version reporting.
# Extracts PySpark version info for `dvt --version` output.

from importlib import metadata as importlib_metadata
from typing import Optional


def get_compute_msg() -> Optional[str]:
    """Get compute engine version information (e.g., pyspark)."""
    compute_engines = []

    # Check for pyspark
    try:
        pyspark_version = importlib_metadata.version("pyspark")
        compute_engines.append(("pyspark", pyspark_version))
    except importlib_metadata.PackageNotFoundError:
        pass

    if not compute_engines:
        return None

    msg_lines = ["Compute:"]
    for name, version in compute_engines:
        msg_lines.append(f"  - {name}: {version}")

    return "\n".join(msg_lines)
