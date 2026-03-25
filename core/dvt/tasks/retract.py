"""
DVT Retract Task — drops all models from their targets in reverse DAG order.

Traverses the DAG downstream-first (reverse topological order) and drops
every model's materialized table/view from its target database. Seeds and
sources are never touched. Also cleans up model result tables from the
DuckDB cache.

Supports all dbt selection operators: --select, --exclude, +model+,
tag:, path:, etc.
"""

import logging
import os
import time
from graphlib import TopologicalSorter
from typing import Any, Dict, List, Optional, Set

from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.runnable import GraphRunnableTask

from dvt.sync.profiles_reader import default_profiles_dir, read_profiles_yml
from dvt.tasks.docs import _get_connection, _get_output_config

logger = logging.getLogger(__name__)

# Engines that support CASCADE on DROP
CASCADE_ENGINES = {"postgres", "redshift", "snowflake", "oracle", "duckdb"}


class DvtRetractTask(GraphRunnableTask):
    """Drop all models from their targets in reverse DAG order."""

    def get_node_selector(self) -> ResourceTypeSelector:
        if self.manifest is None or self.graph is None:
            from dbt_common.exceptions import DbtInternalError

            raise DbtInternalError(
                "manifest and graph must be set to perform node selection"
            )
        return ResourceTypeSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
            resource_types=[NodeType.Model],
        )

    def run(self) -> List[Dict[str, Any]]:
        # Use dbt's full selector infrastructure for --select / --exclude
        self._runtime_initialize()
        selected_uids = self._get_selected_uids()

        project_dir = getattr(self.args, "PROJECT_DIR", None) or "."
        profiles_dir = (
            getattr(self.args, "PROFILES_DIR", None) or default_profiles_dir()
        )

        try:
            profiles = read_profiles_yml(profiles_dir)
        except Exception as e:
            logger.error(f"dvt retract: cannot read profiles.yml: {e}")
            return []

        default_target = self.config.target_name

        # Collect selected model nodes
        model_nodes = {}
        for uid in selected_uids:
            node = self.manifest.nodes.get(uid)
            if node and getattr(node, "resource_type", "") == "model":
                model_nodes[uid] = node

        if not model_nodes:
            print("\n  No models selected for retraction.\n")
            return []

        # Reverse topological order (downstream first)
        ordered_uids = self._reverse_topo_sort(model_nodes)

        total = len(ordered_uids)
        results = []

        print()
        print(f"  Retracting {total} model(s) (reverse DAG order)...")
        print()

        for idx, uid in enumerate(ordered_uids, 1):
            node = model_nodes[uid]
            start = time.time()

            # Resolve target
            config_obj = getattr(node, "config", None)
            model_target = ""
            if config_obj:
                try:
                    model_target = config_obj["target"] or ""
                except (KeyError, TypeError):
                    model_target = getattr(config_obj, "target", "") or ""
            target_name = model_target or default_target

            # Resolve table name
            target_config = _get_output_config(target_name, profiles)
            if not target_config:
                elapsed = time.time() - start
                msg = f"target '{target_name}' not found in profiles"
                print(f"  {idx} of {total}  SKIP {node.name} ({msg})")
                results.append({"uid": uid, "status": "skip", "msg": msg})
                continue

            adapter_type = target_config.get("type", "")
            table_name = self._resolve_table_name(
                node, target_name, default_target, target_config
            )
            materialization = node.get_materialization()
            obj_type = "VIEW" if materialization == "view" else "TABLE"

            # Drop from target (always CASCADE for safety)
            status, msg = self._drop_from_target(
                adapter_type, target_config, table_name, obj_type
            )
            elapsed = time.time() - start

            # Format output
            target_label = (
                f"  ({target_name})" if target_name != default_target else ""
            )
            pad = "." * max(1, 50 - len(node.name) - len(obj_type))
            if status == "ok":
                print(
                    f"  {idx} of {total}  DROP {obj_type} {node.name} "
                    f"{pad} [OK in {elapsed:.1f}s]{target_label}"
                )
            elif status == "skip":
                print(
                    f"  {idx} of {total}  DROP {obj_type} {node.name} "
                    f"{pad} [SKIP]{target_label}"
                )
            else:
                print(
                    f"  {idx} of {total}  DROP {obj_type} {node.name} "
                    f"{pad} [ERROR: {msg}]{target_label}"
                )

            results.append(
                {"uid": uid, "status": status, "msg": msg, "elapsed": elapsed}
            )

        # Clean DuckDB cache for retracted models
        cache_cleared = self._clean_cache(project_dir, model_nodes)

        # Summary
        ok_count = sum(1 for r in results if r["status"] == "ok")
        err_count = sum(1 for r in results if r["status"] == "error")
        skip_count = sum(1 for r in results if r["status"] == "skip")
        total_time = sum(r.get("elapsed", 0) for r in results)

        print()
        print(
            f"  Retract complete: {ok_count} dropped, {cache_cleared} cache "
            f"cleared, {skip_count} skipped, {err_count} errors "
            f"in {total_time:.1f}s"
        )
        print()

        return results

    def interpret_results(self, results) -> bool:
        if isinstance(results, list):
            return all(r.get("status") != "error" for r in results)
        return True

    def _get_selected_uids(self) -> Set[str]:
        """Get selected node UIDs using dbt's full selector infrastructure."""
        spec = self.get_selection_spec()
        selector = self.get_node_selector()
        return selector.get_selected(spec)

    @staticmethod
    def _reverse_topo_sort(model_nodes: Dict[str, Any]) -> List[str]:
        """Sort model UIDs in reverse topological order (downstream first)."""
        model_uids = set(model_nodes.keys())

        graph: Dict[str, set] = {uid: set() for uid in model_uids}
        for uid, node in model_nodes.items():
            depends_on = getattr(node, "depends_on", None)
            if depends_on:
                for dep_uid in getattr(depends_on, "nodes", []) or []:
                    if dep_uid in model_uids:
                        graph[uid].add(dep_uid)

        try:
            sorter = TopologicalSorter(graph)
            ordered = list(sorter.static_order())
            ordered.reverse()
            return ordered
        except Exception:
            return list(model_uids)

    @staticmethod
    def _resolve_table_name(
        node: Any,
        target_name: str,
        default_target: str,
        target_config: Dict[str, Any],
    ) -> str:
        """Get fully-qualified table name for a model on its target."""
        schema = getattr(node, "schema", "")

        if target_name != default_target:
            target_schema = target_config.get(
                "schema", target_config.get("database", "")
            )
            if target_schema:
                schema = target_schema

        if schema:
            return f"{schema}.{node.name}"
        return node.name

    @staticmethod
    def _drop_from_target(
        adapter_type: str,
        target_config: Dict[str, Any],
        table_name: str,
        obj_type: str,
    ) -> tuple:
        """Drop a table/view from the target. Always uses CASCADE."""
        conn = _get_connection(adapter_type, target_config)
        if not conn:
            return ("skip", f"cannot connect to {adapter_type}")

        cascade = " CASCADE" if adapter_type in CASCADE_ENGINES else ""
        drop_sql = f"DROP {obj_type} IF EXISTS {table_name}{cascade}"

        try:
            cursor = conn.cursor()
            cursor.execute(drop_sql)
            conn.commit()
            cursor.close()
            conn.close()
            return ("ok", "")
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            return ("error", str(e)[:200])

    @staticmethod
    def _clean_cache(project_dir: str, model_nodes: Dict[str, Any]) -> int:
        """Drop cached model result tables from DuckDB for retracted models."""
        from dvt.federation.dvt_cache import DvtCache

        cache_path = os.path.join(project_dir, ".dvt", "cache.duckdb")
        if not os.path.isfile(cache_path):
            return 0

        try:
            cache = DvtCache(cache_path)
            tables = cache.list_tables()
            # Only clean cache for models that were retracted
            model_names = {
                getattr(n, "name", "") for n in model_nodes.values()
            }
            model_tables = [
                t
                for t in tables
                if t.startswith("__model__")
                and t.replace("__model__", "") in model_names
            ]
            for tbl in model_tables:
                cache.drop_table(tbl)
            cache.close_and_release()
            return len(model_tables)
        except Exception as e:
            logger.debug(f"dvt retract: cache cleanup failed: {e}")
            return 0
