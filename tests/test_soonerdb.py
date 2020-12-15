import pytest


class TestMemtable:

    def test_empty(self, db):
        assert db.is_empty()

    def test_put(self, db):
        db.put("a", "some value")
        assert db.get("a") == "some value"

    def test_delete(self, db):
        db.put("a", "some value")
        db.delete("a")
        assert db.get("a") is None
        assert db.is_empty()

    def test_clear(self, db):
        db.put("a", "some value")
        db.put("b", "other value")
        db.clear()
        assert db.get("a") is None
        assert db.get("b") is None
        assert db.is_empty()

    def test_key_order(self, db):
        db.put("s", "Sam")
        db.put("g", "Gandalf")
        db.put("f", "Frodo")
        db.put("l", "Legolas")

        assert list(db.items()) == [
            ("f", "Frodo"),
            ("g", "Gandalf"),
            ("l", "Legolas"),
            ("s", "Sam"),
        ]
