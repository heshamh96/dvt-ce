"""
DVT Seed Task — loads CSV seeds via Sling.

Overrides dbt's SeedTask to use DvtSeedRunner (Sling-based)
instead of dbt's SeedRunner (agate-based INSERT batching).
"""

import logging
from typing import Optional, Type

from dbt.task.base import BaseRunner
from dbt.task.seed import SeedTask

from dvt.runners.seed_runner import DvtSeedRunner
from dvt.sync.profiles_reader import default_profiles_dir, read_profiles_yml

logger = logging.getLogger(__name__)


class DvtSeedTask(SeedTask):
    """Seed task using Sling for bulk CSV loading."""

    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        self._raw_profiles: Optional[dict] = None

    def _load_profiles(self) -> dict:
        if self._raw_profiles is None:
            profiles_dir = (
                getattr(self.args, "PROFILES_DIR", None) or default_profiles_dir()
            )
            try:
                self._raw_profiles = read_profiles_yml(profiles_dir)
            except Exception as e:
                logger.warning(f"DVT: Could not read profiles.yml for seed: {e}")
                self._raw_profiles = {}
        return self._raw_profiles

    def get_runner_type(self, _) -> Optional[Type[BaseRunner]]:
        return DvtSeedRunner

    def get_runner(self, node):
        runner = super().get_runner(node)
        if isinstance(runner, DvtSeedRunner):
            runner.set_dvt_context(
                profiles=self._load_profiles(),
                target_name=self.config.target_name,
            )
        return runner
