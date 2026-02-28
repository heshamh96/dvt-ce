# coding=utf-8
"""
Reverse shim: make ``dbt.*`` imports resolve to ``dvt.*`` (or ``dbt_common.*``)
so that third-party adapters (dbt-mysql, dbt-sqlserver, dbt-oracle, etc.) that
still import from ``dbt.exceptions``, ``dbt.version``, ``dbt.contracts.*``, etc.
can work without the upstream ``dbt-core`` package being installed.

The ``dbt.adapters.*`` namespace is handled separately by ``dvt/adapters/__init__.py``
and is **not** touched here. Similarly, ``dbt.include.*`` works as a native
namespace package and needs no intervention.

Usage
-----
Import this module early (from ``dvt/__init__.py``) so the ``sys.meta_path``
finder is registered before any adapter code runs::

    import dvt.dbt_shim  # noqa: F401  – registers the finder

Mapping rules (in priority order)
---------------------------------
1. ``dbt.adapters.*`` → skip (already handled)
2. ``dbt.include.*``  → skip (namespace package, already on disk)
3. ``dbt_common.*``   → skip (already installed)
4. ``dbt.clients.*``  → ``dbt_common.clients.*``
5. ``dbt.events``     → synthetic module exposing ``AdapterLogger``
6. ``dbt.utils``      → synthetic module merging ``dvt.utils`` + ``dbt_common.utils``
7. ``dbt.contracts.connection`` → ``dbt.adapters.contracts.connection``
8. ``dbt.*``          → ``dvt.*``  (catch-all for exceptions, version, contracts.*, context.*, etc.)
"""

import importlib
import importlib.abc
import importlib.machinery
import sys
import types
from typing import Optional


# ---------------------------------------------------------------------------
# Explicit mapping overrides.  Keys are *full* module names that should be
# redirected to a target that isn't simply s/dbt/dvt/.
# ---------------------------------------------------------------------------
_EXPLICIT_REDIRECTS = {
    # dbt.clients.* was moved to dbt_common.clients.*
    # (handled by prefix rule below, but listed for documentation)
}


class _DbtShimFinder(importlib.abc.MetaPathFinder):
    """Intercept ``dbt.*`` imports and redirect to ``dvt.*`` or ``dbt_common.*``."""

    # Prefixes we must NOT touch (they have their own resolution mechanism)
    _SKIP_PREFIXES = (
        "dbt.adapters.",
        "dbt.include.",
        "dbt_common.",
        "dbt_adapters.",
    )

    # Exact top-level names we must NOT touch
    _SKIP_EXACT = frozenset(
        {
            "dbt.adapters",
            "dbt.include",
        }
    )

    def find_spec(self, fullname: str, path, target=None):
        # Only handle dbt.* (but not dbt_common, dbt_adapters, dbt_extractor, …)
        if not fullname.startswith("dbt."):
            return None

        # Skip namespaces handled elsewhere
        if fullname in self._SKIP_EXACT:
            return None
        for prefix in self._SKIP_PREFIXES:
            if fullname.startswith(prefix):
                return None

        # Already resolved — nothing to do
        if fullname in sys.modules:
            return None

        target_name = self._resolve(fullname)
        if target_name is None:
            return None

        return importlib.machinery.ModuleSpec(
            fullname,
            _DbtShimLoader(target_name),
        )

    @staticmethod
    def _resolve(fullname: str) -> Optional[str]:
        """Return the real module name to import, or None to skip."""

        # --- Rule 4: dbt.clients.* → dbt_common.clients.* ---
        if fullname.startswith("dbt.clients."):
            return "dbt_common.clients." + fullname[len("dbt.clients.") :]
        if fullname == "dbt.clients":
            return "dbt_common.clients"

        # --- Rule 7: dbt.contracts.connection → dbt.adapters.contracts.connection ---
        if fullname == "dbt.contracts.connection":
            return "dbt.adapters.contracts.connection"

        # --- Rule 5: dbt.events → synthetic (handled specially in loader) ---
        if fullname == "dbt.events":
            return "__synthetic__.dbt.events"

        # --- Rule 6: dbt.utils → synthetic (handled specially in loader) ---
        if fullname == "dbt.utils":
            return "__synthetic__.dbt.utils"

        # --- Rule 8: catch-all dbt.* → dvt.* ---
        return "dvt" + fullname[3:]


class _DbtShimLoader(importlib.abc.Loader):
    """Load a ``dbt.*`` name by importing its resolved target."""

    def __init__(self, target_name: str):
        self.target_name = target_name

    def create_module(self, spec):
        if self.target_name == "__synthetic__.dbt.events":
            return self._create_events_module(spec.name)
        if self.target_name == "__synthetic__.dbt.utils":
            return self._create_utils_module(spec.name)

        mod = importlib.import_module(self.target_name)
        sys.modules[spec.name] = mod
        return mod

    def exec_module(self, module):
        # Module is already fully initialised.
        pass

    # -- Synthetic module builders -----------------------------------------

    @staticmethod
    def _create_events_module(name: str) -> types.ModuleType:
        """Build a synthetic ``dbt.events`` that exposes ``AdapterLogger``.

        Third-party adapters do ``from dbt.events import AdapterLogger``.
        In the new architecture, ``AdapterLogger`` lives in
        ``dbt.adapters.events.logging``.
        """
        mod = types.ModuleType(
            name, "Shim: dbt.events -> AdapterLogger from dbt-adapters"
        )
        try:
            from dbt.adapters.events.logging import AdapterLogger

            mod.AdapterLogger = AdapterLogger
        except ImportError:
            pass
        sys.modules[name] = mod
        return mod

    @staticmethod
    def _create_utils_module(name: str) -> types.ModuleType:
        """Build a synthetic ``dbt.utils`` that merges ``dvt.utils`` + ``dbt_common.utils``.

        Third-party adapters do ``from dbt.utils import executor``.
        ``executor`` is in ``dbt_common.utils``.  Other utils may be in ``dvt.utils``.
        """
        mod = types.ModuleType(name, "Shim: dbt.utils -> dvt.utils + dbt_common.utils")

        # Layer 1: everything from dbt_common.utils
        try:
            import dbt_common.utils as _dcu

            for attr in dir(_dcu):
                if not attr.startswith("_"):
                    setattr(mod, attr, getattr(_dcu, attr))
        except ImportError:
            pass

        # Layer 2: overlay with dvt.utils (dvt takes precedence)
        try:
            import dvt.utils as _dvtu

            for attr in dir(_dvtu):
                if not attr.startswith("_"):
                    setattr(mod, attr, getattr(_dvtu, attr))
        except ImportError:
            pass

        sys.modules[name] = mod
        return mod


# Register the finder — insert *after* the dvt.adapters finder (position 1)
# so that dvt.adapters.* resolution takes priority.
sys.meta_path.insert(1, _DbtShimFinder())
