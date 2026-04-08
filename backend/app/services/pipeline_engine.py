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
from app.services.csv_service import CsvPreprocessArtifactStore, register_csv_source
from app.services.duckdb_manager import DuckDBManager
from app.services.oracle_service import OracleService
from app.services.postgres_service import PostgresService


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def make_connecting_result(node_id: str, started_at: str) -> NodeExecutionResult:
    return NodeExecutionResult(
        node_id=node_id,
        status=NodeStatus.CONNECTING,
        started_at=started_at,
    )


class PipelineEngine:
    def __init__(
        self,
        duckdb_manager: DuckDBManager,
        csv_artifact_store: CsvPreprocessArtifactStore,
    ):
        self.duckdb = duckdb_manager
        self.csv_artifact_store = csv_artifact_store
        self.oracle = OracleService()
        self.postgres = PostgresService()
        self.node_results: dict[str, NodeExecutionResult] = {}

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

    async def execute_pipeline(
        self,
        pipeline: PipelineDefinition,
        force_refresh: bool = False,
        on_node_start: Callable[[str, str], None] | None = None,
        on_node_finish: Callable[[NodeExecutionResult], None] | None = None,
        on_node_update: Callable[[NodeExecutionResult], None] | None = None,
    ) -> dict[str, NodeExecutionResult]:
        order = self.topological_sort(pipeline)
        node_map = {n.id: n for n in pipeline.nodes}
        results: dict[str, NodeExecutionResult] = {}

        for node_id in order:
            node = node_map[node_id]
            if not force_refresh and self.duckdb.table_exists(node.table_name):
                if node_id in self.node_results:
                    results[node_id] = self.node_results[node_id]
                    continue

            started_at = utc_now_iso()
            if on_node_start is not None:
                on_node_start(node.id, started_at)
            if node.type == NodeType.DB_SOURCE and on_node_update is not None:
                on_node_update(make_connecting_result(node.id, started_at))
            result = await self._execute_node(node, started_at=started_at)
            results[node_id] = result
            self.node_results[node_id] = result
            if on_node_finish is not None:
                on_node_finish(result)

        return results

    async def execute_single_node(
        self,
        node: NodeDefinition,
        on_node_start: Callable[[str, str], None] | None = None,
        on_node_finish: Callable[[NodeExecutionResult], None] | None = None,
        on_node_update: Callable[[NodeExecutionResult], None] | None = None,
    ) -> NodeExecutionResult:
        started_at = utc_now_iso()
        if on_node_start is not None:
            on_node_start(node.id, started_at)
        if node.type == NodeType.DB_SOURCE and on_node_update is not None:
            on_node_update(make_connecting_result(node.id, started_at))
        result = await self._execute_node(node, started_at=started_at)
        self.node_results[node.id] = result
        if on_node_finish is not None:
            on_node_finish(result)
        return result

    async def _execute_node(
        self,
        node: NodeDefinition,
        *,
        started_at: str | None = None,
    ) -> NodeExecutionResult:
        start = time.time()
        effective_started_at = started_at or utc_now_iso()
        try:
            if node.type == NodeType.CSV_SOURCE:
                stats = await asyncio.to_thread(
                    register_csv_source,
                    self.duckdb,
                    node.id,
                    node.table_name,
                    node.config,
                    self.csv_artifact_store,
                )
            elif node.type == NodeType.DB_SOURCE:
                db_type = node.config.get("db_type", "postgres")
                svc = self.oracle if db_type == "oracle" else self.postgres
                connection = await svc.connect(node.config)
                query_started_at = utc_now_iso()
                try:
                    df = await svc.fetch_query(connection, node.config["query"])
                finally:
                    close = getattr(connection, "close")
                    maybe_awaitable = close()
                    if asyncio.iscoroutine(maybe_awaitable):
                        await maybe_awaitable
                stats = await asyncio.to_thread(
                    self.duckdb.register_dataframe,
                    node.table_name,
                    df,
                )
            elif node.type == NodeType.TRANSFORM:
                stats = await asyncio.to_thread(
                    self.duckdb.execute_transform,
                    node.table_name, node.config["sql"]
                )
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
                started_at=query_started_at if node.type == NodeType.DB_SOURCE else effective_started_at,
                finished_at=utc_now_iso(),
            )
        except Exception as e:
            elapsed = (time.time() - start) * 1000
            return NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.ERROR,
                error=str(e),
                execution_time_ms=elapsed,
                started_at=effective_started_at,
                finished_at=utc_now_iso(),
            )
