"""Node cache-key fingerprints.

A node's cache key hashes everything that determines its output: its own
config (only the result-affecting subset) plus the cache keys of its
upstreams. Editing a source therefore changes the keys of every descendant
(Merkle propagation), so 'is this table still valid?' is a pure comparison
against the key recorded in _shori_node_meta when the table was built.

Deliberately excluded from keys:
- connection passwords (secrets; rotating one doesn't change the data)
- oracle fetch_config (changes how data is fetched, not what it is)
- table_name, label, position (presentation/storage naming only)
"""

import hashlib
import json

from app.models.pipeline import NodeType, PipelineDefinition

from app.services.csv_service import _normalize_preprocessing


def _connection_identity(config: dict) -> dict:
    connection = config.get("connection") or {}
    return {
        "host": connection.get("host"),
        "port": connection.get("port"),
        "database": connection.get("database"),
        "service_name": connection.get("service_name"),
        "user": connection.get("user"),
    }


def _own_payload(node) -> dict:
    config = node.config or {}
    if node.type == NodeType.DB_SOURCE:
        return {
            "db_type": config.get("db_type", "postgres"),
            "connection": _connection_identity(config),
            "query": config.get("query"),
        }
    if node.type == NodeType.CSV_SOURCE:
        return {
            "file_path": config.get("file_path"),
            "original_filename": config.get("original_filename"),
            "preprocessing": _safe_preprocessing(config.get("preprocessing")),
        }
    if node.type == NodeType.EXCEL_SOURCE:
        return {
            "file_path": config.get("materialized_csv_path"),
            "selected_sheet": config.get("selected_sheet"),
            "preprocessing": _safe_preprocessing(config.get("preprocessing")),
        }
    if node.type == NodeType.TRANSFORM:
        return {"sql": config.get("sql")}
    return {}


def _safe_preprocessing(preprocessing) -> dict | None:
    try:
        return _normalize_preprocessing(preprocessing)
    except ValueError:
        # Invalid preprocessing config still distinguishes the node from a
        # valid one; execution will surface the real error.
        return {"invalid": str(preprocessing)}


def compute_cache_keys(pipeline: PipelineDefinition) -> dict[str, str]:
    """Cache key per node id, walking the DAG so upstream keys feed downstream."""
    upstream_ids: dict[str, list[str]] = {node.id: [] for node in pipeline.nodes}
    for edge in pipeline.edges:
        if edge.target in upstream_ids:
            upstream_ids[edge.target].append(edge.source)

    keys: dict[str, str] = {}
    node_map = {node.id: node for node in pipeline.nodes}

    def key_for(node_id: str, visiting: frozenset) -> str:
        if node_id in keys:
            return keys[node_id]
        if node_id in visiting:
            raise ValueError("Pipeline contains a cycle")
        node = node_map[node_id]
        upstream_keys = sorted(
            key_for(upstream, visiting | {node_id})
            for upstream in upstream_ids[node_id]
            if upstream in node_map
        )
        payload = json.dumps(
            {
                "type": node.type.value,
                "config": _own_payload(node),
                "upstream": upstream_keys,
            },
            sort_keys=True,
            default=str,
        )
        keys[node_id] = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        return keys[node_id]

    for node in pipeline.nodes:
        key_for(node.id, frozenset())
    return keys
