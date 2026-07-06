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
