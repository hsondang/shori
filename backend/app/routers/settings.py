from fastapi import APIRouter, HTTPException

from app.models.pipeline import (
    DatabaseConnectionDefinition,
    DatabaseConnectionInputDefinition,
)
from app.storage.pipeline_store import (
    GlobalConnectionInUseError,
    PipelineStore,
    SavedConnectionNameConflictError,
)

router = APIRouter()


def get_store() -> PipelineStore:
    return PipelineStore()


@router.get("/database-connections")
def list_global_database_connections() -> list[DatabaseConnectionDefinition]:
    return get_store().list_global_connections()


@router.post("/database-connections")
def create_global_database_connection(
    connection: DatabaseConnectionInputDefinition,
) -> DatabaseConnectionDefinition:
    try:
        return get_store().create_global_connection(connection)
    except SavedConnectionNameConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.put("/database-connections/{connection_id}")
def update_global_database_connection(
    connection_id: str,
    connection: DatabaseConnectionInputDefinition,
) -> DatabaseConnectionDefinition:
    try:
        return get_store().update_global_connection(connection_id, connection)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Global database connection not found")
    except SavedConnectionNameConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc))


@router.delete("/database-connections/{connection_id}")
def delete_global_database_connection(connection_id: str):
    try:
        deleted = get_store().delete_global_connection(connection_id)
    except GlobalConnectionInUseError as exc:
        raise HTTPException(status_code=409, detail=str(exc))

    if not deleted:
        raise HTTPException(status_code=404, detail="Global database connection not found")

    return {"ok": True}
