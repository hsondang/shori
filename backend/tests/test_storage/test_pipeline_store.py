import sqlite3

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
                auto_label="CSV Source",
                label_mode="custom",
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
    assert loaded.nodes[0].auto_label == "CSV Source"
    assert loaded.nodes[0].label_mode == "custom"


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
    assert items[0]["starred"] is False
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


def test_update_star_reorders_projects(store):
    store.save(_make_pipeline("p1", "First"))
    store.save(_make_pipeline("p2", "Second"))

    updated = store.update_star("p1", True)

    assert updated is True
    items = store.list_all()
    assert items[0]["id"] == "p1"
    assert items[0]["starred"] is True
    assert items[1]["id"] == "p2"
    assert items[1]["starred"] is False


def test_update_star_returns_false_for_missing_project(store):
    assert store.update_star("missing-project", True) is False


def test_existing_database_is_migrated_to_include_starred(monkeypatch, tmp_path):
    project_db_path = tmp_path / "projects.sqlite3"
    conn = sqlite3.connect(project_db_path)
    conn.execute(
        """
        CREATE TABLE projects (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            pipeline_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    pipeline = _make_pipeline()
    conn.execute(
        """
        INSERT INTO projects (id, name, pipeline_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            pipeline.id,
            pipeline.name,
            pipeline.model_dump_json(indent=2),
            "2026-03-01T00:00:00+00:00",
            "2026-03-01T00:00:00+00:00",
        ),
    )
    conn.commit()
    conn.close()

    import app.storage.pipeline_store as pipeline_store_module

    monkeypatch.setattr(pipeline_store_module, "PROJECT_DB_PATH", project_db_path)
    migrated_store = PipelineStore()

    items = migrated_store.list_all()
    assert items == [
        {
            "id": "p1",
            "name": "Test Pipeline",
            "starred": False,
            "created_at": "2026-03-01T00:00:00+00:00",
            "updated_at": "2026-03-01T00:00:00+00:00",
        }
    ]
