"""DVT FederationModelRunner — executes models via cross-target Spark federation.

This runner is used when a model has dependencies on sources/models from different
targets than its own target.  The execution path is:
1. Compile (Jinja resolution) — handled by parent CompileRunner
2. Extract sources to staging (with predicate pushdown)
3. Transform in Spark (SQLGlot translation)
4. Load to target (via bucket or JDBC)
"""

from __future__ import annotations

import threading
from typing import Optional

from dvt.adapters.base import BaseAdapter
from dvt.artifacts.schemas.results import RunStatus
from dvt.artifacts.schemas.run import RunResult
from dvt.config import RuntimeConfig
from dvt.contracts.graph.manifest import Manifest
from dvt.contracts.graph.nodes import ModelNode
from dvt.dvt_runners.runner_mixin import DvtRunnerMixin
from dvt.federation.engine import FederationEngine
from dvt.federation.resolver import ResolvedExecution
from dvt.task import group_lookup
from dvt.task.compile import CompileRunner
from dbt_common.events.base_types import EventLevel
from dbt_common.events.functions import fire_event


class FederationModelRunner(DvtRunnerMixin, CompileRunner):
    """Runner for models that require cross-target federation."""

    def __init__(
        self,
        config: RuntimeConfig,
        adapter: BaseAdapter,
        node: ModelNode,
        node_index: int,
        num_nodes: int,
        resolution: ResolvedExecution,
        manifest: Manifest,
        federation_semaphore: Optional[threading.Semaphore] = None,
    ):
        super().__init__(config, adapter, node, node_index, num_nodes)
        self._init_dvt_runner(resolution, manifest)
        self._federation_engine: Optional[FederationEngine] = None
        self._federation_semaphore = federation_semaphore

    def describe_node(self) -> str:
        upstream_targets = ", ".join(sorted(self.resolution.upstream_targets))
        return (
            f"{self.node.language} federation model {self.get_node_representation()} "
            f"[{upstream_targets}] -> {self.resolution.target}"
        )

    def compile(self, manifest: Manifest):
        """Compile with default adapter — no source adapter override.

        Federation models always compile with the DEFAULT adapter (the
        profile's primary target). This is correct because:
        1. {{ source() }} and {{ ref() }} create relations using the default
           adapter's Relation class, avoiding cross-adapter validation errors
           (e.g., MySQL rejects database != schema for PG source relations).
        2. The federation engine handles SQL translation/transpilation in
           _translate_to_spark() — it rewrites table refs to Spark temp views,
           making the compiled SQL dialect irrelevant.
        3. The compiled SQL just needs valid table references that the
           federation engine can find and replace.

        We call compiler.compile_node() with adapter=None and skip_source_adapter=True
        to prevent the DvtCompiler from resolving a non-default source adapter
        based on model.config.target.
        """
        extra_context = self._resolve_is_incremental_for_federation(manifest)
        return self.compiler.compile_node(
            self.node, manifest, extra_context, skip_source_adapter=True
        )

    def execute(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Execute model via federation engine.

        If a federation semaphore is set, the actual Spark work is gated
        behind it so that at most ``max_federation_threads`` federation
        models execute concurrently through the shared JVM.  The runner
        is still submitted to the thread pool immediately (DAG order
        preserved); it simply blocks here until a slot is available.
        """
        from dvt.events.types import LogModelResult
        from dbt_common.events.types import Formatting

        # Log materialization coercion warning if needed
        if self.resolution.requires_materialization_coercion:
            fire_event(
                LogModelResult(
                    description=(
                        f"Materialization coerced: {self.resolution.original_materialization} "
                        f"-> {self.resolution.coerced_materialization} "
                        f"(cross-target federation cannot create views)"
                    ),
                    status="WARN",
                    index=self.node_index,
                    total=self.num_nodes,
                    execution_time=0,
                    node_info=model.node_info,
                    group=group_lookup.get(model.unique_id),
                ),
                level=EventLevel.WARN,
            )

        # --- Federation concurrency gate ---
        sem = self._federation_semaphore
        if sem is not None:
            fire_event(
                Formatting(
                    msg=f"  Federation slot requested for {model.name} "
                    f"(thread {threading.current_thread().name})"
                )
            )
            sem.acquire()
            fire_event(Formatting(msg=f"  Federation slot acquired for {model.name}"))

        try:
            return self._run_federation(model, manifest)
        finally:
            if sem is not None:
                sem.release()
                fire_event(
                    Formatting(msg=f"  Federation slot released for {model.name}")
                )

    def _run_federation(self, model: ModelNode, manifest: Manifest) -> RunResult:
        """Core federation execution — extract, transform, load."""
        from dbt_common.events.types import Formatting

        def on_progress(msg: str) -> None:
            fire_event(Formatting(msg=f"  {msg}"))

        engine = FederationEngine(
            runtime_config=self.config,
            manifest=manifest,
            on_progress=on_progress,
        )

        result = engine.execute(
            model=model,
            resolution=self.resolution,
            compiled_sql=model.compiled_code,
        )

        if result.get("success", False):
            return RunResult(
                node=model,
                status=RunStatus.Success,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=result.get("execution_time", 0),
                message=result.get("message", "OK"),
                adapter_response={
                    "federation": True,
                    "method": result.get("load_method"),
                },
                failures=0,
                batch_results=None,
            )
        else:
            return RunResult(
                node=model,
                status=RunStatus.Error,
                timing=[],
                thread_id=threading.current_thread().name,
                execution_time=result.get("execution_time", 0),
                message=result.get("message", "FAILED"),
                adapter_response={"federation": True, "error": result.get("error")},
                failures=1,
                batch_results=None,
            )
