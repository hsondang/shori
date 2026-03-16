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


def test_load_legacy_pipeline_without_database_connections(store, tmp_path, monkeypatch):
    legacy_path = tmp_path / "legacy.json"
    legacy_path.write_text(
        """
        {
          "id": "legacy",
          "name": "Legacy Pipeline",
          "nodes": [
            {
              "id": "n1",
              "type": "csv_source",
              "table_name": "t",
              "label": "CSV",
              "position": {"x": 0, "y": 0},
              "config": {
                "file_path": "/tmp/f.csv",
                "original_filename": "f.csv"
              }
            }
          ],
          "edges": []
        }
        """
    )

    import app.storage.pipeline_store as ps_mod

    monkeypatch.setattr(ps_mod, "PIPELINE_DIR", tmp_path)
    loaded = store.load("legacy")
    assert loaded.database_connections == []


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
    ids = {item["id"] for item in items}
    assert ids == {"p1", "p2"}
    names = {item["name"] for item in items}
    assert names == {"First", "Second"}


def test_delete(store):
    pipeline = _make_pipeline()
    store.save(pipeline)
    store.delete(pipeline.id)
    with pytest.raises(FileNotFoundError):
        store.load(pipeline.id)


def test_delete_nonexistent(store):
    store.delete("does-not-exist")  # should not raise
