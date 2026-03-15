"""
Verify and bootstrap Sling binary.

The `sling` Python package (pip install sling) bundles/downloads the Go binary
to ~/.sling/bin/. It exposes the path via sling.SLING_BIN. The binary may not
be on PATH but is still usable through the Python package.
"""

import os
import shutil
import subprocess
import sys
from typing import Optional, Tuple


def check_sling() -> Tuple[bool, Optional[str]]:
    """Check if Sling is available and return (available, version_string).

    Checks in order:
    1. sling.SLING_BIN (Python package's bundled binary)
    2. sling on PATH

    Returns:
        (True, "1.5.12") if Sling is available
        (False, None) if Sling is not found
    """
    sling_bin = _find_sling_bin()
    if not sling_bin:
        return False, None

    return _get_sling_version(sling_bin)


def ensure_sling(dry_run: bool = False) -> Tuple[bool, Optional[str], str]:
    """Ensure Sling binary is installed. Bootstraps via Python package if needed.

    Returns:
        (available, version, status)
        status is one of: 'installed', 'bootstrapped', 'not_found', 'dry_run'
    """
    # Check if already available
    sling_bin = _find_sling_bin()
    if sling_bin:
        available, version = _get_sling_version(sling_bin)
        if available:
            return True, version, "installed"

    if dry_run:
        return False, None, "dry_run"

    # Try to bootstrap: importing sling triggers binary download
    bootstrapped = _bootstrap_sling()
    if bootstrapped:
        sling_bin = _find_sling_bin()
        if sling_bin:
            available, version = _get_sling_version(sling_bin)
            if available:
                return True, version, "bootstrapped"

    return False, None, "not_found"


def _find_sling_bin() -> Optional[str]:
    """Find the sling binary path.

    Priority:
    1. sling.SLING_BIN (from the Python package — most reliable)
    2. sling on PATH
    """
    # Method 1: Python package's SLING_BIN
    try:
        from sling import SLING_BIN

        if SLING_BIN and os.path.isfile(SLING_BIN):
            return SLING_BIN
    except (ImportError, Exception):
        pass

    # Method 2: PATH
    path_bin = shutil.which("sling")
    if path_bin:
        return path_bin

    return None


def _bootstrap_sling() -> bool:
    """Bootstrap the Sling binary via the Python sling package.

    Importing sling and accessing SLING_BIN triggers the binary download
    if it hasn't happened yet.
    """
    try:
        result = subprocess.run(
            [sys.executable, "-c", "from sling import SLING_BIN; print(SLING_BIN)"],
            capture_output=True,
            text=True,
            timeout=120,  # binary download may take time
        )
        if result.returncode == 0 and result.stdout.strip():
            path = result.stdout.strip()
            return os.path.isfile(path)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        pass

    return False


def _get_sling_version(sling_bin: str) -> Tuple[bool, Optional[str]]:
    """Get sling version from the binary."""
    try:
        result = subprocess.run(
            [sling_bin, "--version"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        output = result.stdout.strip() or result.stderr.strip()
        for part in output.split():
            if part.startswith("v") or (part and part[0].isdigit()):
                return True, part.lstrip("v")
        return True, output
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False, None
