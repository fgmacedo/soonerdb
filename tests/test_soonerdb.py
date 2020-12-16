
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


class TestWAL:

    def test_empty(self, db, tmpdir):
        wal_log = db.wal.path
        assert wal_log.exists()
        assert wal_log.read_bytes() == b''

    def test_put(self, db):
        db.put("vinicity", "Mordor")
        wal_log = db.wal.path
        assert wal_log.exists()
        assert wal_log.read_bytes() == b'\x08\x00\x00\x00vinicity\x06\x00\x00\x00Mordor'

    def test_restore_from_wal(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)
        db.put("a", "some value")
        db.put("b", "other value")
        db.put("S", "São Paulo")

        db = SoonerDB(tmpdir)
        assert db.get("a") == "some value"
        assert db.get("b") == "other value"
        assert db.get("S") == "São Paulo"

    def test_restore_from_wal_with_overriden_values(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)
        db.put("a", "Frodo")
        db.put("b", "Gandalf")
        db.put("a", "Sam")

        db = SoonerDB(tmpdir)
        wal_log = db.wal.path
        assert db.get("a") == "Sam"
        assert db.get("b") == "Gandalf"

        assert wal_log.read_bytes() == (
            b'\x01\x00\x00\x00a\x05\x00\x00\x00Frodo\x01\x00\x00\x00b\x07\x00\x00\x00Ganda'
            b'lf\x01\x00\x00\x00a\x03\x00\x00\x00Sam'
        )
