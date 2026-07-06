import type { Edge, Node } from '@xyflow/react'

/**
 * Structural-vs-data edge semantics (docs/excel-node-model.md §3.1).
 *
 * An edge whose source is an Excel workbook hub is *structural*: it records
 * workbook→sheet lineage for the canvas but is not a data dependency. It is
 * excluded from execution/ancestor walks, cannot be deleted independently,
 * and dies with its sheet node. Frontend mirror of pipeline_graph.py — the
 * distinction is derived from the source node's type, never an edge field.
 */
export function isStructuralEdge(
  edge: Pick<Edge, 'source'>,
  nodesById: Map<string, Node>,
): boolean {
  return nodesById.get(edge.source)?.type === 'excel_workbook'
}

export function buildNodesById(nodes: Node[]): Map<string, Node> {
  return new Map(nodes.map((node) => [node.id, node]))
}

/** Only the edges that carry data — what dependency/ancestor walks must see. */
export function dataEdges<E extends Pick<Edge, 'source'>>(
  edges: E[],
  nodes: Node[],
): E[] {
  const nodesById = buildNodesById(nodes)
  return edges.filter((edge) => !isStructuralEdge(edge, nodesById))
}
