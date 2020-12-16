
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
        wal_log = db._wal.path
        assert wal_log.exists()
        assert wal_log.read_bytes() == b''

    def test_put(self, db):
        db.put("vinicity", "Mordor")
        wal_log = db._wal.path
        assert wal_log.exists()
        assert wal_log.read_bytes() == b'\x08\x00\x00\x00vinicity\x06\x00\x00\x00Mordor'

    def test_restore_from_wal(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)
        db.put("a", "some value")
        db.put("b", "other value")
        db.put("S", "São Paulo")
        del db

        db = SoonerDB(tmpdir)
        assert db.get("a") == "some value"
        assert db.get("b") == "other value"
        assert db.get("S") == "São Paulo"

    def test_restore_from_wal_with_overriden_values(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)
        db.put("a", "Frodo")
        db.put("b", "Gandalf")
        db.put("a", "Sam")
        del db

        db = SoonerDB(tmpdir)
        wal_log = db._wal.path
        assert db.get("a") == "Sam"
        assert db.get("b") == "Gandalf"

        assert wal_log.read_bytes() == (
            b'\x01\x00\x00\x00a\x05\x00\x00\x00Frodo\x01\x00\x00\x00b\x07\x00\x00\x00Ganda'
            b'lf\x01\x00\x00\x00a\x03\x00\x00\x00Sam'
        )


class TestSSTable:
    """
    We're using a key based on numerical index only to ease stressing the boundaries of
    memtable limits.
    """

    def test_dump_to_sstable(self, db, tmpdir):
        for i in range(db.memtable_items_limit + 1):
            db.put(f"{i}", f"{i*2}")

        assert len(db._memtable) == 1

    def test_get_using_sstable(self, db, tmpdir):
        for i in range(db.memtable_items_limit + 1):
            db.put(f"{i}", f"{i*2}")

        for i in range(db.memtable_items_limit + 1):
            assert db.get(f"{i}") == f"{i*2}"

    def test_restore_sstable(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)

        for i in range(db.memtable_items_limit + 1):
            db.put(f"{i}", f"{i*2}")

        del db

        db = SoonerDB(tmpdir)
        for i in range(db.memtable_items_limit + 1):
            assert db.get(f"{i}") == f"{i*2}"

        # the next key is none
        i += 1
        assert db.get(f"{i}") is None

    def test_get_value_from_the_last_ssfile(self, db):
        # one pass i*2 -> file1
        for i in range(db.memtable_items_limit):
            db.put(f"{i}", f"{i*2}")

        # one pass i*3 -> file2
        for i in range(db.memtable_items_limit):
            db.put(f"{i}", f"{i*3}")

        # one pass i*4 -> file3
        for i in range(db.memtable_items_limit):
            db.put(f"{i}", f"{i*4}")

        # then check that the value returned if for i*4
        for i in range(db.memtable_items_limit):
            assert db.get(f"{i}") == f"{i*4}"
