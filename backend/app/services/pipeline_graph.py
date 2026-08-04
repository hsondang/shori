"""Structural-vs-data edge semantics (docs/excel-node-model.md §3.1).

An edge whose source is an Excel workbook hub is *structural*: it records
workbook→sheet lineage for the canvas but is not a data dependency. The engine
and cache keys must both see only data edges, and both build the same
upstream-ids map, so the filter lives here — the single authority for the
"filter by source node type, not an edge field" decision (spec decision 3).
"""

from app.models.pipeline import EdgeDefinition, NodeDefinition, NodeType, PipelineDefinition


def is_structural_edge(edge: EdgeDefinition, node_map: dict[str, NodeDefinition]) -> bool:
    source = node_map.get(edge.source)
    return source is not None and source.type == NodeType.EXCEL_WORKBOOK


def data_upstream_ids(pipeline: PipelineDefinition) -> dict[str, list[str]]:
    """Upstream node ids per node id, excluding structural (workbook→sheet) edges."""
    node_map = {node.id: node for node in pipeline.nodes}
    upstream_ids: dict[str, list[str]] = {node.id: [] for node in pipeline.nodes}
    for edge in pipeline.edges:
        if edge.target not in upstream_ids:
            continue
        if is_structural_edge(edge, node_map):
            continue
        upstream_ids[edge.target].append(edge.source)
    return upstream_ids


def upstream_table_name(pipeline: PipelineDefinition, node_id: str) -> str | None:
    """The table name of a node's single data upstream, or None.

    Terminal nodes (export) take exactly one input, so the first data edge is
    the answer — this mirrors what the canvas shows on the node card.
    """
    node_map = {n.id: n for n in pipeline.nodes}
    for edge in pipeline.edges:
        if edge.target != node_id or is_structural_edge(edge, node_map):
            continue
        upstream = node_map.get(edge.source)
        if upstream is not None and upstream.table_name:
            return upstream.table_name
    return None


def resolve_direct_upstreams(
    pipeline: PipelineDefinition,
    node_id: str,
    cache_keys: dict[str, str | None],
    manager,
) -> tuple[dict[str, str], list[str]]:
    """Resolve each direct upstream of a node to its consumable copy.

    Returns (table_name -> location, missing_table_names). Anything reading
    upstream tables by name — a transform's live preview, an export's SQL —
    has to pin the precedence-chosen copy (spec §6) rather than trust the
    search path, or it silently reads a stale materialized table. Callers
    surface `missing` as the 409 `upstreams_unavailable` contract.
    """
    node_map = {n.id: n for n in pipeline.nodes}
    resolution: dict[str, str] = {}
    missing: list[str] = []
    for edge in pipeline.edges:
        if edge.target != node_id:
            continue
        if is_structural_edge(edge, node_map):
            continue
        upstream = node_map.get(edge.source)
        if upstream is None:
            continue
        location = manager.consumable_location(edge.source, cache_keys.get(edge.source))
        if location is None:
            missing.append(upstream.table_name)
        else:
            resolution[upstream.table_name] = location
    return resolution, missing
