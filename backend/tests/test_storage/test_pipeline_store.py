import pytest

from app.models.pipeline import PipelineDefinition, NodeDefinition, NodeType, Position
from app.storage.pipeline_store import PipelineStore


def _make_pipeline(pid="p1", name="Test Pipeline"):
    return PipelineDefinition(
        id=pid,
        name=name,
        database_connections=[
            {
                "id": "conn-1",
                "name": "Analytics",
                "db_type": "postgres",
                "host": "localhost",
                "port": 5432,
                "database": "analytics",
                "user": "user",
                "password": "secret",
            }
        ],
        nodes=[
            NodeDefinition(
                id="n1",
                type=NodeType.CSV_SOURCE,
                table_name="t",
                label="CSV",
                position=Position(x=0, y=0),
                config={"file_path": "/tmp/f.csv", "original_filename": "f.csv"},
            )
        ],
        edges=[],
    )


@pytest.fixture
def store():
    return PipelineStore()


def test_save_and_load_roundtrip(store):
    pipeline = _make_pipeline()
    store.save(pipeline)
    loaded = store.load(pipeline.id)
    assert loaded.id == pipeline.id
    assert loaded.name == pipeline.name
    assert len(loaded.database_connections) == 1
    assert loaded.database_connections[0].name == "Analytics"
    assert len(loaded.nodes) == 1
    assert loaded.nodes[0].type == NodeType.CSV_SOURCE


def test_load_not_found(store):
    with pytest.raises(FileNotFoundError):
        store.load("nonexistent-id")


def test_list_all_empty(store):
    assert store.list_all() == []


def test_list_all_multiple(store):
    store.save(_make_pipeline("p1", "First"))
    store.save(_make_pipeline("p2", "Second"))
    items = store.list_all()
    assert len(items) == 2
    assert items[0]["id"] == "p2"
    assert items[1]["id"] == "p1"
    assert items[0]["name"] == "Second"
    assert items[0]["created_at"]
    assert items[0]["updated_at"]


def test_save_update_preserves_created_at_and_refreshes_updated_at(store):
    pipeline = _make_pipeline("p1", "First")
    store.save(pipeline)

    first = store.list_all()[0]

    updated_pipeline = _make_pipeline("p1", "Renamed")
    store.save(updated_pipeline)

    second = store.list_all()[0]
    assert second["id"] == "p1"
    assert second["name"] == "Renamed"
    assert second["created_at"] == first["created_at"]
    assert second["updated_at"] >= first["updated_at"]


def test_delete(store):
    pipeline = _make_pipeline()
    store.save(pipeline)
    store.delete(pipeline.id)
    with pytest.raises(FileNotFoundError):
        store.load(pipeline.id)


def test_delete_nonexistent(store):
    store.delete("does-not-exist")  # should not raise
