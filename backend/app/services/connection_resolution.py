"""Resolving 'global' saved connections into inline node connection configs."""

from fastapi import HTTPException

from app.models.pipeline import (
    DatabaseConnectionDefinition,
    NodeDefinition,
    PipelineDefinition,
)
from app.storage.pipeline_store import PipelineStore


def saved_connection_to_config(connection: DatabaseConnectionDefinition) -> dict:
    if connection.db_type == "oracle":
        return {
            "host": connection.host,
            "port": connection.port,
            "service_name": connection.service_name,
            "user": connection.user,
            "password": connection.password,
        }

    return {
        "host": connection.host,
        "port": connection.port,
        "database": connection.database,
        "user": connection.user,
        "password": connection.password,
    }


def resolve_node_connections(node: NodeDefinition, store: PipelineStore) -> NodeDefinition:
    config = dict(node.config)
    if node.type != "db_source" or config.get("connection_mode") != "global":
        return node

    connection_source_id = config.get("connection_source_id")
    if not isinstance(connection_source_id, str) or not connection_source_id:
        raise HTTPException(status_code=400, detail="Global database source is missing connection_source_id")

    try:
        connection = store.load_global_connection(connection_source_id)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Global database connection not found")

    return node.model_copy(update={
        "config": {
            **config,
            "db_type": connection.db_type,
            "connection": saved_connection_to_config(connection),
        }
    })


def resolve_pipeline_connections(pipeline: PipelineDefinition, store: PipelineStore) -> PipelineDefinition:
    return pipeline.model_copy(update={
        "nodes": [resolve_node_connections(node, store) for node in pipeline.nodes],
    })
