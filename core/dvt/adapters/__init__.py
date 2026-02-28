"""
dvt.adapters: re-export dbt.adapters so DVT can use the existing dbt-adapters package.
Install dbt-adapters (e.g. dbt-postgres) alongside dvt-core; this module exposes it as dvt.adapters.

Uses a sys.meta_path finder to intercept all ``dvt.adapters.*`` imports and
resolve them to the same module objects as ``dbt.adapters.*``.  This guarantees
class identity is preserved (e.g. ``dvt…QueryComment is dbt…QueryComment``),
which is required for ``isinstance`` checks in the upstream dbt-adapters code.
"""

import importlib
import importlib.abc
import importlib.machinery
import sys

import dbt.adapters as _dbt_adapters

# Top-level alias — ``import dvt.adapters`` returns the real dbt.adapters module.
sys.modules["dvt.adapters"] = _dbt_adapters


class _DvtAdaptersFinder(importlib.abc.MetaPathFinder):
    """Redirect ``dvt.adapters.*`` imports to ``dbt.adapters.*``."""

    def find_spec(self, fullname, path, target=None):
        if not fullname.startswith("dvt.adapters."):
            return None

        # Already aliased — nothing to do.
        if fullname in sys.modules:
            return None

        dbt_name = "dbt" + fullname[3:]  # dvt.adapters.X → dbt.adapters.X

        return importlib.machinery.ModuleSpec(
            fullname,
            _DvtAdaptersLoader(dbt_name),
        )


class _DvtAdaptersLoader(importlib.abc.Loader):
    """Load a ``dvt.adapters.*`` name by importing the ``dbt.adapters.*`` equivalent."""

    def __init__(self, dbt_name: str):
        self.dbt_name = dbt_name

    def create_module(self, spec):
        mod = importlib.import_module(self.dbt_name)
        # Also register under the dvt name so subsequent lookups are instant.
        sys.modules[spec.name] = mod
        return mod

    def exec_module(self, module):
        # Module is already fully initialised — nothing to execute.
        pass


sys.meta_path.insert(0, _DvtAdaptersFinder())
