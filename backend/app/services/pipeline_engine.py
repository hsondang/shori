import asyncio
import time
from collections import defaultdict, deque
from collections.abc import Callable
from datetime import datetime, timezone

from app.models.pipeline import (
    NodeDefinition,
    NodeExecutionResult,
    NodeStatus,
    NodeType,
    PipelineDefinition,
)
from app.services.cache_keys import compute_cache_keys
from app.services.csv_service import CsvPreprocessArtifactStore, register_csv_source
from app.services.duckdb_manager import DuckDBManager, validate_user_table_name
from app.services.execution_registry import ExecutionCancelled, ExecutionController
from app.services.oracle_service import OracleService, normalize_fetch_config
from app.services.postgres_service import PostgresService

DEFAULT_MAX_CONCURRENT_NODES = 4


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_connecting_result(node_id: str, started_at: str) -> NodeExecutionResult:
    return NodeExecutionResult(
        node_id=node_id,
        status=NodeStatus.CONNECTING,
        started_at=started_at,
    )


def make_running_result(node_id: str, started_at: str) -> NodeExecutionResult:
    return NodeExecutionResult(
        node_id=node_id,
        status=NodeStatus.RUNNING,
        started_at=started_at,
    )


def make_upstream_failed_result(node_id: str, started_at: str) -> NodeExecutionResult:
    finished_at = utc_now_iso()
    return NodeExecutionResult(
        node_id=node_id,
        status=NodeStatus.CANCELLED,
        error="Upstream node failed or was cancelled.",
        started_at=started_at,
        finished_at=finished_at,
    )


class PipelineEngine:
    def __init__(
        self,
        duckdb_manager: DuckDBManager,
        csv_artifact_store: CsvPreprocessArtifactStore,
        *,
        max_concurrent_nodes: int | None = None,
    ):
        self.duckdb = duckdb_manager
        self.csv_artifact_store = csv_artifact_store
        self.oracle = OracleService()
        self.postgres = PostgresService()
        self.max_concurrent_nodes = max_concurrent_nodes or DEFAULT_MAX_CONCURRENT_NODES

    def topological_sort(self, pipeline: PipelineDefinition) -> list[str]:
        adj: dict[str, list[str]] = defaultdict(list)
        in_degree = {n.id: 0 for n in pipeline.nodes}
        for edge in pipeline.edges:
            adj[edge.source].append(edge.target)
            in_degree[edge.target] += 1

        queue = deque(nid for nid, deg in in_degree.items() if deg == 0)
        order: list[str] = []
        while queue:
            node_id = queue.popleft()
            order.append(node_id)
            for neighbor in adj[node_id]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

        if len(order) != len(pipeline.nodes):
            raise ValueError("Pipeline contains a cycle")
        return order

    def _validate_table_names(self, pipeline: PipelineDefinition) -> None:
        seen: dict[str, str] = {}
        for node in pipeline.nodes:
            if node.type == NodeType.EXPORT:
                continue
            validate_user_table_name(node.table_name)
            other = seen.get(node.table_name)
            if other is not None:
                raise ValueError(
                    f"Nodes '{other}' and '{node.id}' both use table name "
                    f"'{node.table_name}'; table names must be unique within a project."
                )
            seen[node.table_name] = node.id

    def cached_result(self, node: NodeDefinition, cache_key: str | None) -> NodeExecutionResult | None:
        """Return the persisted result if the node's table is current, else None."""
        if node.type == NodeType.EXPORT or cache_key is None:
            return None
        meta = self.duckdb.get_node_meta(node.id)
        if meta is None or meta["status"] != "complete" or meta["cache_key"] != cache_key:
            return None
        if meta["table_name"] != node.table_name:
            # User renamed the output table; the data itself is still valid.
            try:
                self.duckdb.rename_node_table(node.id, node.table_name)
            except Exception:
                return None
        if not self.duckdb.table_exists(node.table_name):
            return None
        return NodeExecutionResult(
            node_id=node.id,
            status=NodeStatus.SUCCESS,
            row_count=meta["row_count"],
            column_count=meta["column_count"],
            columns=meta["columns"],
            execution_time_ms=meta["duration_ms"],
            started_at=meta["started_at"],
            finished_at=meta["finished_at"],
            cached=True,
        )

    async def execute_pipeline(
        self,
        pipeline: PipelineDefinition,
        force_refresh: bool = False,
        on_node_start: Callable[[str, str], None] | None = None,
        on_node_finish: Callable[[NodeExecutionResult], None] | None = None,
        on_node_update: Callable[[NodeExecutionResult], None] | None = None,
        execution_controller: ExecutionController | None = None,
    ) -> dict[str, NodeExecutionResult]:
        order = self.topological_sort(pipeline)
        self._validate_table_names(pipeline)
        cache_keys = compute_cache_keys(pipeline)
        node_map = {n.id: n for n in pipeline.nodes}
        upstream_ids: dict[str, list[str]] = {n.id: [] for n in pipeline.nodes}
        for edge in pipeline.edges:
            upstream_ids[edge.target].append(edge.source)

        results: dict[str, NodeExecutionResult] = {}

        for node_id in order:
            if execution_controller is not None:
                execution_controller.raise_if_cancelled()
            node = node_map[node_id]

            upstream_results = [results.get(up) for up in upstream_ids[node_id]]
            if any(r is None or r.status != NodeStatus.SUCCESS for r in upstream_results):
                result = make_upstream_failed_result(node_id, utc_now_iso())
                results[node_id] = result
                if on_node_finish is not None:
                    on_node_finish(result)
                continue

            if not force_refresh:
                cached = self.cached_result(node, cache_keys.get(node_id))
                if cached is not None:
                    results[node_id] = cached
                    if on_node_finish is not None:
                        on_node_finish(cached)
                    continue

            started_at = utc_now_iso()
            if on_node_start is not None:
                on_node_start(node.id, started_at)
            if node.type == NodeType.DB_SOURCE and on_node_update is not None:
                on_node_update(make_connecting_result(node.id, started_at))
            result = await self._execute_node(
                node,
                cache_key=cache_keys.get(node_id),
                started_at=started_at,
                on_node_update=on_node_update,
                execution_controller=execution_controller,
            )
            results[node_id] = result
            if on_node_finish is not None:
                on_node_finish(result)

        return results

    async def execute_single_node(
        self,
        node: NodeDefinition,
        *,
        cache_key: str | None = None,
        force_refresh: bool = False,
        on_node_start: Callable[[str, str], None] | None = None,
        on_node_finish: Callable[[NodeExecutionResult], None] | None = None,
        on_node_update: Callable[[NodeExecutionResult], None] | None = None,
        execution_controller: ExecutionController | None = None,
    ) -> NodeExecutionResult:
        if execution_controller is not None:
            execution_controller.raise_if_cancelled()
        if not force_refresh:
            cached = self.cached_result(node, cache_key)
            if cached is not None:
                if on_node_finish is not None:
                    on_node_finish(cached)
                return cached
        started_at = utc_now_iso()
        if on_node_start is not None:
            on_node_start(node.id, started_at)
        if node.type == NodeType.DB_SOURCE and on_node_update is not None:
            on_node_update(make_connecting_result(node.id, started_at))
        result = await self._execute_node(
            node,
            cache_key=cache_key,
            started_at=started_at,
            on_node_update=on_node_update,
            execution_controller=execution_controller,
        )
        if on_node_finish is not None:
            on_node_finish(result)
        return result

    async def _execute_node(
        self,
        node: NodeDefinition,
        *,
        cache_key: str | None = None,
        started_at: str | None = None,
        on_node_update: Callable[[NodeExecutionResult], None] | None = None,
        execution_controller: ExecutionController | None = None,
    ) -> NodeExecutionResult:
        start = time.time()
        effective_started_at = started_at or utc_now_iso()
        current_started_at = effective_started_at
        try:
            if execution_controller is not None:
                execution_controller.raise_if_cancelled()
            if node.type == NodeType.CSV_SOURCE:
                stats = await asyncio.to_thread(
                    register_csv_source,
                    self.duckdb,
                    node.id,
                    node.table_name,
                    node.config,
                    self.csv_artifact_store,
                    cache_key,
                )
            elif node.type == NodeType.EXCEL_SOURCE:
                materialized_csv_path = str(node.config.get("materialized_csv_path", "")).strip()
                selected_sheet = str(node.config.get("selected_sheet", "")).strip()
                if not selected_sheet:
                    raise ValueError("Excel source is missing a selected_sheet")
                if not materialized_csv_path:
                    raise ValueError("Excel source is missing a materialized_csv_path")
                csv_config = {
                    **node.config,
                    "file_path": materialized_csv_path,
                    "original_filename": node.config.get("materialized_csv_filename")
                    or f"{selected_sheet}.csv",
                }
                stats = await asyncio.to_thread(
                    register_csv_source,
                    self.duckdb,
                    node.id,
                    node.table_name,
                    csv_config,
                    self.csv_artifact_store,
                    cache_key,
                )
            elif node.type == NodeType.DB_SOURCE:
                db_type = node.config.get("db_type", "postgres")
                svc = self.oracle if db_type == "oracle" else self.postgres
                connection = await svc.connect(node.config)
                if execution_controller is not None:
                    execution_controller.raise_if_cancelled()
                    execution_controller.set_abort_callback(
                        node.id, lambda: svc.abort_query(connection)
                    )
                query_started_at = utc_now_iso()
                current_started_at = query_started_at
                if on_node_update is not None:
                    on_node_update(make_running_result(node.id, query_started_at))
                try:
                    if execution_controller is not None:
                        execution_controller.raise_if_cancelled()
                    if db_type == "oracle":
                        stats = await self.oracle.load_query_to_duckdb(
                            connection,
                            node.config["query"],
                            node.table_name,
                            self.duckdb,
                            node.config.get("fetch_config"),
                            node_id=node.id,
                            cache_key=cache_key,
                        )
                    else:
                        df = await svc.fetch_query(connection, node.config["query"])
                        stats = await asyncio.to_thread(
                            self.duckdb.register_dataframe,
                            node.table_name,
                            df,
                            node_id=node.id,
                            cache_key=cache_key,
                        )
                finally:
                    if execution_controller is not None:
                        execution_controller.clear_abort_callback(node.id)
                    close = getattr(connection, "close")
                    maybe_awaitable = close()
                    if asyncio.iscoroutine(maybe_awaitable):
                        await maybe_awaitable
            elif node.type == NodeType.TRANSFORM:
                def register_interrupt(interrupt):
                    if execution_controller is not None:
                        execution_controller.set_abort_callback(node.id, interrupt)

                try:
                    stats = await asyncio.to_thread(
                        self.duckdb.execute_transform,
                        node.table_name,
                        node.config["sql"],
                        node_id=node.id,
                        cache_key=cache_key,
                        register_interrupt=register_interrupt,
                    )
                finally:
                    if execution_controller is not None:
                        execution_controller.clear_abort_callback(node.id)
            elif node.type == NodeType.EXPORT:
                stats = {"row_count": 0, "column_count": 0, "columns": []}
            else:
                raise ValueError(f"Unknown node type: {node.type}")

            elapsed = (time.time() - start) * 1000
            return NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.SUCCESS,
                row_count=stats["row_count"],
                column_count=stats["column_count"],
                columns=stats["columns"],
                execution_time_ms=elapsed,
                started_at=current_started_at,
                finished_at=utc_now_iso(),
            )
        except ExecutionCancelled:
            raise
        except Exception as e:
            elapsed = (time.time() - start) * 1000
            return NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.ERROR,
                error=str(e),
                execution_time_ms=elapsed,
                started_at=current_started_at,
                finished_at=utc_now_iso(),
            )
