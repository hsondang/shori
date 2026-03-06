import time
from collections import defaultdict, deque

from app.models.pipeline import (
    NodeDefinition,
    NodeExecutionResult,
    NodeStatus,
    NodeType,
    PipelineDefinition,
)
from app.services.duckdb_manager import DuckDBManager
from app.services.oracle_service import OracleService
from app.services.postgres_service import PostgresService


class PipelineEngine:
    def __init__(self, duckdb_manager: DuckDBManager):
        self.duckdb = duckdb_manager
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
        self, pipeline: PipelineDefinition, force_refresh: bool = False
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

            result = await self._execute_node(node)
            results[node_id] = result
            self.node_results[node_id] = result

        return results

    async def execute_single_node(
        self, node: NodeDefinition
    ) -> NodeExecutionResult:
        result = await self._execute_node(node)
        self.node_results[node.id] = result
        return result

    async def _execute_node(self, node: NodeDefinition) -> NodeExecutionResult:
        start = time.time()
        try:
            if node.type == NodeType.CSV_SOURCE:
                stats = self.duckdb.register_csv(
                    node.table_name, node.config["file_path"]
                )
            elif node.type == NodeType.DB_SOURCE:
                db_type = node.config.get("db_type", "postgres")
                svc = self.oracle if db_type == "oracle" else self.postgres
                df = await svc.execute_query(node.config)
                stats = self.duckdb.register_dataframe(node.table_name, df)
            elif node.type == NodeType.TRANSFORM:
                stats = self.duckdb.execute_transform(
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
            )
        except Exception as e:
            elapsed = (time.time() - start) * 1000
            return NodeExecutionResult(
                node_id=node.id,
                status=NodeStatus.ERROR,
                error=str(e),
                execution_time_ms=elapsed,
            )
