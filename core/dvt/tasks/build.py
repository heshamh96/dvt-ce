"""
DVT Build Task — extends dbt's BuildTask with cross-engine extraction.

BuildTask inherits from RunTask, so by extending from DvtRunTask,
we get all the extraction path resolution for free.
"""

from typing import Optional, Type

from dbt.task.build import BuildTask

from dvt.tasks.run import DvtRunTask


class DvtBuildTask(DvtRunTask, BuildTask):
    """BuildTask with DVT extraction paths.

    MRO: DvtBuildTask → DvtRunTask → BuildTask → RunTask → ...
    DvtRunTask provides get_runner_type() and get_runner() overrides.
    BuildTask provides the build-specific run logic (seeds, tests, snapshots in DAG order).
    """

    pass
