"""Cache-key isolation for the Excel workbook hub (docs/excel-node-model.md §6).

Structural workbook→sheet edges must contribute nothing to any sheet node's
Merkle key: editing the hub, or adding/removing the structural edge itself,
never invalidates a sheet's cached table.
"""

from app.models.pipeline import (
    EdgeDefinition,
    NodeDefinition,
    NodeType,
    PipelineDefinition,
    Position,
)
from app.services.cache_keys import compute_cache_keys


def _make_node(node_id, node_type, table_name, config=None):
    return NodeDefinition(
        id=node_id,
        type=node_type,
        table_name=table_name,
        label=node_id,
        position=Position(x=0, y=0),
        config=config or {},
    )


def _make_pipeline(nodes, edges=None):
    edge_objs = [
        EdgeDefinition(id=f"e{i}", source=s, target=t)
        for i, (s, t) in enumerate(edges or [])
    ]
    return PipelineDefinition(id="p1", name="Test", nodes=nodes, edges=edge_objs)


def _hub(file_path="wb.xlsx"):
    return _make_node("hub", NodeType.EXCEL_WORKBOOK, None, {
        "file_path": file_path,
        "original_filename": "wb.xlsx",
        "sheet_names": ["Orders", "Summary"],
    })


def _sheet(node_id, table_name, sheet):
    return _make_node(node_id, NodeType.EXCEL_SOURCE, table_name, {
        "file_path": "wb.xlsx",
        "selected_sheet": sheet,
        "header": True,
    })


def test_hub_config_change_leaves_sheet_keys_unchanged():
    s1 = _sheet("s1", "orders_t", "Orders")
    s2 = _sheet("s2", "summary_t", "Summary")
    edges = [("hub", "s1"), ("hub", "s2")]

    before = compute_cache_keys(_make_pipeline([_hub(), s1, s2], edges))
    after = compute_cache_keys(
        _make_pipeline([_hub(file_path="replaced.xlsx"), s1, s2], edges)
    )

    assert after["s1"] == before["s1"]
    assert after["s2"] == before["s2"]


def test_structural_edge_does_not_feed_sheet_keys():
    s1 = _sheet("s1", "orders_t", "Orders")

    with_edge = compute_cache_keys(_make_pipeline([_hub(), s1], [("hub", "s1")]))
    without_edge = compute_cache_keys(_make_pipeline([_hub(), s1]))
    orphan = compute_cache_keys(_make_pipeline([s1]))

    assert with_edge["s1"] == without_edge["s1"] == orphan["s1"]


def test_data_edges_still_propagate():
    s1 = _sheet("s1", "orders_t", "Orders")
    tx = _make_node("tx", NodeType.TRANSFORM, "tx_t", {"sql": "SELECT * FROM orders_t"})
    edges = [("hub", "s1"), ("s1", "tx")]

    before = compute_cache_keys(_make_pipeline([_hub(), s1, tx], edges))
    changed_sheet = _make_node("s1", NodeType.EXCEL_SOURCE, "orders_t", {
        "file_path": "wb.xlsx",
        "selected_sheet": "Orders",
        "header": False,
    })
    after = compute_cache_keys(_make_pipeline([_hub(), changed_sheet, tx], edges))

    # Editing the sheet still invalidates its downstream (Merkle propagation).
    assert after["s1"] != before["s1"]
    assert after["tx"] != before["tx"]
