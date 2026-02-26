import shutil

import pytest

from jsonldb import Database


@pytest.fixture
def db_path(tmp_path):
    db_dir = tmp_path / "testdb"
    yield str(db_dir)
    if db_dir.exists():
        shutil.rmtree(db_dir)


def test_insert_and_get(db_path):
    db = Database(db_path, index_fields=["email"])

    record_id = db.insert({"name": "Alice", "email": "alice@example.com"})
    assert record_id is not None

    record = db.get(record_id)
    assert record is not None
    assert record.data["name"] == "Alice"
    assert record.data["email"] == "alice@example.com"


def test_query_by_index(db_path):
    db = Database(db_path, index_fields=["email", "age"])

    db.insert({"name": "Alice", "email": "alice@example.com", "age": 30})
    db.insert({"name": "Bob", "email": "bob@example.com", "age": 30})
    db.insert({"name": "Charlie", "email": "charlie@example.com", "age": 25})

    results = list(db.query("age", 30))
    assert len(results) == 2


def test_update(db_path):
    db = Database(db_path)

    record_id = db.insert({"name": "Alice", "age": 30})
    db.update(record_id, {"age": 31})

    record = db.get(record_id)
    assert record.data["age"] == 31
    assert record.data["name"] == "Alice"


def test_delete(db_path):
    db = Database(db_path)

    record_id = db.insert({"name": "Alice"})
    assert db.get(record_id) is not None

    db.delete(record_id)
    assert db.get(record_id) is None


def test_count(db_path):
    db = Database(db_path)

    assert db.count() == 0
    db.insert({"name": "Alice"})
    db.insert({"name": "Bob"})
    assert db.count() == 2


def test_all(db_path):
    db = Database(db_path)

    db.insert({"name": "Alice"})
    db.insert({"name": "Bob"})

    records = list(db.all())
    assert len(records) == 2
    names = {r.data["name"] for r in records}
    assert names == {"Alice", "Bob"}


def test_compaction(db_path):
    db = Database(db_path, index_fields=["email"])

    id1 = db.insert({"name": "Alice", "email": "alice@example.com"})
    id2 = db.insert({"name": "Bob", "email": "bob@example.com"})
    db.insert({"name": "Charlie", "email": "charlie@example.com"})
    db.delete(id2)
    db.update(id1, {"name": "Alice Updated"})

    assert db.get(id2) is None

    compacted_count = db.compact()

    assert compacted_count == 2

    assert db.get(id1).data["name"] == "Alice Updated"
    assert db.get(id2) is None
    assert db.get(db.insert({"name": "New"})).data["name"] == "New"

    results = list(db.query("email", "alice@example.com"))
    assert len(results) == 1
