import asyncio
import logging
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
from app.services.postgres_service import PostgresAttachUnavailable, PostgresService

logger = logging.getLogger(__name__)

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


def compute_upstream_resolution(
    pipeline: PipelineDefinition,
    node_id: str,
    cache_keys: dict[str, str],
    duckdb: DuckDBManager,
) -> dict[str, str]:
    """Map each direct upstream's table_name → the location a downstream read
    should resolve to (spec §6 precedence). Only upstreams that have a present
    consumable copy are included; the empty case means 'search-path default'."""
    node_map = {node.id: node for node in pipeline.nodes}
    resolution: dict[str, str] = {}
    for edge in pipeline.edges:
        if edge.target != node_id:
            continue
        upstream = node_map.get(edge.source)
        if upstream is None:
            continue
        location = duckdb.consumable_location(edge.source, cache_keys.get(edge.source))
        if location is not None:
            resolution[upstream.table_name] = location
    return resolution


class PipelineEngine:
    def __init__(
        self,
        duckdb_manager: DuckDBManager,
        csv_artifact_store: CsvPreprocessArtifactStore,
        *,
        connection_pools=None,
        max_concurrent_nodes: int | None = None,
        max_connections_per_database: int | None = None,
        use_postgres_attach: bool = False,
    ):
        self.duckdb = duckdb_manager
        self.csv_artifact_store = csv_artifact_store
        self.oracle = OracleService()
        self.postgres = PostgresService()
        self.connection_pools = connection_pools
        self.max_concurrent_nodes = max_concurrent_nodes or DEFAULT_MAX_CONCURRENT_NODES
        self.max_connections_per_database = max_connections_per_database
        # DuckDB-attaches the postgres source and runs CTAS itself (no data
        # through Python). Opt-in: it may download the postgres extension on
        # first use, which unit-test environments shouldn't do.
        self.use_postgres_attach = use_postgres_attach

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

    @staticmethod
    def node_into_memory(node: NodeDefinition) -> bool:
        """True when the node should load into the RAM-only scratch catalog.

        Default is in-memory (the exploratory/draft mode); a node only persists
        to the project file when its config explicitly sets load_mode=materialized.
        """
        return node.config.get("load_mode", "in_memory") != "materialized"

    def cached_result(self, node: NodeDefinition, cache_key: str | None) -> NodeExecutionResult | None:
        """Return the persisted result if the node's table is current, else None."""
        if node.type == NodeType.EXPORT or cache_key is None:
            return None
        requested_location = "in_memory" if self.node_into_memory(node) else "materialized"
        # Look up the copy in the node's configured load-mode location specifically:
        # a node may also hold a copy in the other location, but a run targets this one.
        meta = self.duckdb.get_node_meta(node.id, requested_location)
        if meta is None or meta["status"] != "complete" or meta["cache_key"] != cache_key:
            return None
        if meta["table_name"] != node.table_name:
            # User renamed the output table; the data itself is still valid.
            try:
                self.duckdb.rename_node_table(node.id, node.table_name)
            except Exception:
                return None
        if not self.duckdb.table_exists(node.table_name, location=requested_location):
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
        """Run the DAG with independent nodes in parallel.

        Every node gets a task that waits on its upstreams' done events, so
        the DAG itself is the schedule; the semaphore caps how many nodes
        execute simultaneously. Each node writes its own DuckDB table, which
        keeps concurrent writes conflict-free under MVCC.
        """
        self.topological_sort(pipeline)  # cycle validation
        self._validate_table_names(pipeline)
        cache_keys = compute_cache_keys(pipeline)
        node_map = {n.id: n for n in pipeline.nodes}
        upstream_ids: dict[str, list[str]] = {n.id: [] for n in pipeline.nodes}
        for edge in pipeline.edges:
            if edge.target in upstream_ids:
                upstream_ids[edge.target].append(edge.source)

        results: dict[str, NodeExecutionResult] = {}
        done_events = {node_id: asyncio.Event() for node_id in node_map}
        semaphore = asyncio.Semaphore(self.max_concurrent_nodes)

        async def node_task(node_id: str) -> None:
            node = node_map[node_id]
            try:
                for upstream in upstream_ids[node_id]:
                    await done_events[upstream].wait()
                if execution_controller is not None:
                    execution_controller.raise_if_cancelled()

                upstream_results = [results.get(up) for up in upstream_ids[node_id]]
                if any(r is None or r.status != NodeStatus.SUCCESS for r in upstream_results):
                    result = make_upstream_failed_result(node_id, utc_now_iso())
                    results[node_id] = result
                    if on_node_finish is not None:
                        on_node_finish(result)
                    return

                if not force_refresh:
                    cached = await asyncio.to_thread(
                        self.cached_result, node, cache_keys.get(node_id)
                    )
                    if cached is not None:
                        results[node_id] = cached
                        if on_node_finish is not None:
                            on_node_finish(cached)
                        return

                async with semaphore:
                    if execution_controller is not None:
                        execution_controller.raise_if_cancelled()
                    started_at = utc_now_iso()
                    if on_node_start is not None:
                        on_node_start(node.id, started_at)
                    if node.type == NodeType.DB_SOURCE and on_node_update is not None:
                        on_node_update(make_connecting_result(node.id, started_at))
                    upstream_resolution = None
                    if node.type == NodeType.TRANSFORM:
                        upstream_resolution = await asyncio.to_thread(
                            compute_upstream_resolution, pipeline, node_id, cache_keys, self.duckdb
                        )
                    result = await self._execute_node(
                        node,
                        cache_key=cache_keys.get(node_id),
                        started_at=started_at,
                        on_node_update=on_node_update,
                        execution_controller=execution_controller,
                        upstream_resolution=upstream_resolution,
                    )
                    results[node_id] = result
                    if on_node_finish is not None:
                        on_node_finish(result)
            finally:
                done_events[node_id].set()

        try:
            async with asyncio.TaskGroup() as tg:
                for node_id in node_map:
                    tg.create_task(node_task(node_id))
        except* ExecutionCancelled:
            raise ExecutionCancelled()

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
        upstream_resolution: dict[str, str] | None = None,
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
            upstream_resolution=upstream_resolution,
        )
        if on_node_finish is not None:
            on_node_finish(result)
        return result

    async def _load_postgres_node(
        self,
        node: NodeDefinition,
        connection,
        cache_key: str | None,
        execution_controller: ExecutionController | None,
        into_memory: bool = False,
    ) -> dict:
        if self.use_postgres_attach:
            def register_interrupt(interrupt):
                if execution_controller is not None:
                    execution_controller.set_abort_callback(node.id, interrupt)

            try:
                return await self.postgres.load_query_to_duckdb(
                    node.config["connection"],
                    node.config["query"],
                    node.table_name,
                    self.duckdb,
                    node_id=node.id,
                    cache_key=cache_key,
                    into_memory=into_memory,
                    register_interrupt=register_interrupt,
                )
            except PostgresAttachUnavailable:
                pass
            except ExecutionCancelled:
                raise
            except Exception:
                # An aborted run interrupts the DuckDB load, which surfaces
                # here as a generic error — don't rerun the query in that case.
                if execution_controller is not None:
                    execution_controller.raise_if_cancelled()
                # The attach path failed before producing a table (e.g. the
                # extension can't reach the host the way the driver can);
                # the driver connection is already open, so use it.
                logger.warning(
                    "DuckDB attach load failed for node %s; falling back to driver fetch.",
                    node.id,
                    exc_info=True,
                )

        if execution_controller is not None:
            execution_controller.set_abort_callback(
                node.id, lambda: self.postgres.abort_query(connection)
            )
        df = await self.postgres.fetch_query(connection, node.config["query"])
        return await asyncio.to_thread(
            self.duckdb.register_dataframe,
            node.table_name,
            df,
            node_id=node.id,
            cache_key=cache_key,
            into_memory=into_memory,
        )

    async def _execute_node(
        self,
        node: NodeDefinition,
        *,
        cache_key: str | None = None,
        started_at: str | None = None,
        on_node_update: Callable[[NodeExecutionResult], None] | None = None,
        execution_controller: ExecutionController | None = None,
        upstream_resolution: dict[str, str] | None = None,
    ) -> NodeExecutionResult:
        start = time.time()
        effective_started_at = started_at or utc_now_iso()
        current_started_at = effective_started_at
        into_memory = self.node_into_memory(node)
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
                    into_memory,
                )
            elif node.type == NodeType.EXCEL_SOURCE:
                selected_sheet = str(node.config.get("selected_sheet", "")).strip()
                file_path = str(node.config.get("file_path", "")).strip()
                if not selected_sheet:
                    raise ValueError("Excel source is missing a selected_sheet")
                if not file_path:
                    raise ValueError("Excel source is missing a file_path")
                cell_range = str(node.config.get("cell_range", "")).strip() or None
                stats = await asyncio.to_thread(
                    self.duckdb.register_excel,
                    node.table_name,
                    file_path,
                    sheet=selected_sheet,
                    cell_range=cell_range,
                    header=bool(node.config.get("header", True)),
                    all_varchar=bool(node.config.get("all_varchar", False)),
                    node_id=node.id,
                    cache_key=cache_key,
                    into_memory=into_memory,
                )
            elif node.type == NodeType.DB_SOURCE:
                db_type = node.config.get("db_type", "postgres")
                svc = self.oracle if db_type == "oracle" else self.postgres
                release = None
                if self.connection_pools is not None:
                    connection, release = await self.connection_pools.acquire(
                        db_type,
                        node.config["connection"],
                        self.max_connections_per_database,
                    )
                else:
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
                            into_memory=into_memory,
                        )
                    else:
                        stats = await self._load_postgres_node(
                            node, connection, cache_key, execution_controller, into_memory
                        )
                finally:
                    if execution_controller is not None:
                        execution_controller.clear_abort_callback(node.id)
                    if release is not None:
                        await release()
                    else:
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
                        into_memory=into_memory,
                        register_interrupt=register_interrupt,
                        upstream_resolution=upstream_resolution,
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
