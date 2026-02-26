"""DVT ShowTask — extends dbt ShowTask with target-aware routing.

For models targeting non-default adapters (pushdown or federation),
routes to ShowRunnerTargetAware which queries the materialized table
on the correct target adapter.

Uses DvtCompiler on the default ShowRunner path so that cross-dialect
SQL transpilation is applied when previewing models.

DvtShowRunner wraps compiled SQL in a subquery to avoid LIMIT clause
conflicts when the model already contains a LIMIT.
"""

from __future__ import annotations

import threading
import time
from typing import Dict

from dvt.adapters.factory import get_adapter
from dvt.artifacts.schemas.run import RunResult, RunStatus
from dvt.context.providers import generate_runtime_model_context
from dvt.contracts.graph.nodes import SeedNode
from dvt.dvt_compilation.dvt_compiler import DvtCompiler
from dvt.dvt_runners.show_runner_target_aware import ShowRunnerTargetAware
from dvt.federation.resolver import ExecutionPath, FederationResolver, ResolvedExecution
from dvt.task.seed import SeedRunner
from dvt.task.show import ShowRunner, ShowTask


class DvtShowRunner(ShowRunner):
    """ShowRunner that wraps compiled SQL in a subquery to avoid LIMIT conflicts.

    The upstream get_show_sql macro appends its own LIMIT, which produces
    invalid SQL if the model already contains a LIMIT clause (e.g.,
    "... LIMIT 100\\nlimit 10").  Wrapping in a subquery makes the outer
    LIMIT apply cleanly.
    """

    def execute(self, compiled_node, manifest):
        start_time = time.time()

        # Allow passing in -1 (or any negative number) to get all rows
        limit = None if self.config.args.limit < 0 else self.config.args.limit

        model_context = generate_runtime_model_context(
            compiled_node, self.config, manifest
        )

        # DVT: Wrap in subquery to avoid double-LIMIT
        compiled_code = model_context["compiled_code"]
        compiled_code = f"SELECT * FROM ({compiled_code}) AS _dvt_show_subq"

        compiled_node.compiled_code = self.adapter.execute_macro(
            macro_name="get_show_sql",
            macro_resolver=manifest,
            context_override=model_context,
            kwargs={
                "compiled_code": compiled_code,
                "sql_header": model_context["config"].get("sql_header"),
                "limit": limit,
            },
        )
        adapter_response, execute_result = self.adapter.execute(
            compiled_node.compiled_code, fetch=True
        )

        end_time = time.time()

        return RunResult(
            node=compiled_node,
            status=RunStatus.Success,
            timing=[],
            thread_id=threading.current_thread().name,
            execution_time=end_time - start_time,
            message=None,
            adapter_response=adapter_response.to_dict(),
            agate_table=execute_result,
            failures=None,
            batch_results=None,
        )


class DvtShowTask(ShowTask):
    """ShowTask with DVT federation-aware runner routing."""

    def __init__(self, args, config, manifest=None):
        super().__init__(args, config, manifest)
        self._resolved_executions: Dict[str, ResolvedExecution] = {}
        # DVT: Use DvtCompiler for cross-dialect transpilation
        self.compiler = DvtCompiler(self.config)

    def _runtime_initialize(self):
        # Call base (which includes the parent's inline query handling)
        super()._runtime_initialize()

        # DVT: Resolve federation paths for selected models so we can
        # route non-default target models to ShowRunnerTargetAware.
        if self.args.select and self.manifest and self._flattened_nodes:
            try:
                resolver = FederationResolver(
                    manifest=self.manifest,
                    runtime_config=self.config,
                    args=self.args,
                )
                selected_uids = [n.unique_id for n in self._flattened_nodes]
                self._resolved_executions = resolver.resolve_all(selected_uids)
            except Exception:
                # If federation resolution fails, fall back to default behavior
                self._resolved_executions = {}

    def get_runner(self, node):
        """Route to the correct runner based on federation resolution."""
        adapter = get_adapter(self.config)

        if node.is_ephemeral_model:
            run_count = 0
            num_nodes = 0
        else:
            self.run_count += 1
            run_count = self.run_count
            num_nodes = self.num_nodes

        # Check if this is a seed
        if isinstance(node, SeedNode):
            return SeedRunner(self.config, adapter, node, run_count, num_nodes)

        # Check federation resolution
        resolution = self._resolved_executions.get(node.unique_id)
        if resolution:
            is_federation = resolution.execution_path == ExecutionPath.SPARK_FEDERATION
            is_non_default = resolution.target != self.config.target_name

            if is_federation or is_non_default:
                # Federation or non-default target — query the materialized
                # table on the correct target adapter instead of re-compiling.
                try:
                    if is_non_default:
                        from dvt.federation.adapter_manager import AdapterManager

                        profiles_dir = getattr(self.config.args, "PROFILES_DIR", None)
                        if not profiles_dir:
                            profiles_dir = getattr(
                                self.config.args, "profiles_dir", None
                            )
                        profiles_dir = str(profiles_dir) if profiles_dir else None

                        profile_name = self.config.profile_name

                        target_adapter = AdapterManager.get_adapter(
                            profile_name=profile_name,
                            target_name=resolution.target,
                            profiles_dir=profiles_dir,
                        )
                    else:
                        # Federation to default target — materialized table
                        # is on the default adapter
                        target_adapter = adapter

                    return ShowRunnerTargetAware(
                        self.config,
                        adapter,
                        node,
                        run_count,
                        num_nodes,
                        target_adapter,
                        resolution,
                    )
                except Exception:
                    # If adapter creation fails, fall back to default ShowRunner
                    pass

        # Default: same-target pushdown — DvtShowRunner (subquery-wrapping)
        # with DvtCompiler for cross-dialect transpilation support
        runner = DvtShowRunner(self.config, adapter, node, run_count, num_nodes)
        runner.compiler = DvtCompiler(self.config)
        return runner
