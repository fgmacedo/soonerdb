import pytest


class TestMemtable:

    def test_empty(self, db):
        assert db.is_empty()

    def test_put(self, db):
        db.set("a", "some value")
        assert db.get("a") == "some value"

    def test_delete(self, db):
        db.set("a", "some value")
        db.delete("a")
        assert db.get("a", None) is None
        with pytest.raises(KeyError) as e:
            db.get("a")
        assert e.match("not found")
        assert db.is_empty()

    def test_clear_memtable(self, db):
        db.set("a", "some value")
        db.set("b", "other value")
        db.clear()
        assert db.get("a", None) is None
        assert db.get("b", None) is None
        assert db.is_empty()

    def test_key_order(self, db):
        db.set("s", "Sam")
        db.set("g", "Gandalf")
        db.set("f", "Frodo")
        db.set("l", "Legolas")

        assert list(db.items()) == [
            ("f", "Frodo"),
            ("g", "Gandalf"),
            ("l", "Legolas"),
            ("s", "Sam"),
        ]

    def test_path_is_initialized(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir / 'subdir')
        assert db.path.exists()


class TestDictLikeInterface:

    def test_use_as_dict_api(self, db):
        db["a"] = "some value"
        assert db["a"] == "some value"

    def test_contains(self, db):
        db["a"] = "some value"

        assert 'a' in db
        assert 'b' not in db

    def test_delete(self, db):
        db["a"] = "some value"
        del db['a']
        assert 'a' not in db

    def test_items(self, db):
        db["a"] = "some value"
        db["b"] = "other"

        assert list(db.items()) == [
            ("a", "some value"),
            ("b", "other"),
        ]


class TestWAL:

    def test_empty(self, db, tmpdir):
        wal_log = db._wal.path
        assert wal_log.exists()
        assert wal_log.read_bytes() == b''

    def test_put(self, db):
        db.set("vinicity", "Mordor")
        wal_log = db._wal.path
        assert wal_log.exists()
        assert wal_log.read_bytes() == b'\x08\x00\x00\x00vinicity\x06\x00\x00\x00Mordor'

    def test_restore_from_wal(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)
        db.set("a", "some value")
        db.set("b", "other value")
        db.set("S", "São Paulo")
        del db

        db = SoonerDB(tmpdir)
        assert db.get("a") == "some value"
        assert db.get("b") == "other value"
        assert db.get("S") == "São Paulo"

    def test_restore_from_wal_with_overriden_values(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)
        db.set("a", "Frodo")
        db.set("b", "Gandalf")
        db.set("a", "Sam")
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
            db.set(f"{i}", f"{i*2}")

        assert len(db._memtable) == 1

    def test_get_using_sstable(self, db, tmpdir):
        for i in range(db.memtable_items_limit + 1):
            db.set(f"{i}", f"{i*2}")

        for i in range(db.memtable_items_limit + 1):
            assert db.get(f"{i}") == f"{i*2}"

    def test_restore_sstable(self, SoonerDB, tmpdir):
        db = SoonerDB(tmpdir)

        for i in range(db.memtable_items_limit + 1):
            db.set(f"{i}", f"{i*2}")

        del db

        db = SoonerDB(tmpdir)
        for i in range(db.memtable_items_limit + 1):
            assert db.get(f"{i}") == f"{i*2}"

        # the next key is none
        i += 1
        assert db.get(f"{i}", None) is None

    def test_get_value_from_the_last_ssfile(self, db):
        # one pass i*2 -> file1
        for i in range(db.memtable_items_limit):
            db.set(f"{i}", f"{i*2}")

        # one pass i*3 -> file2
        for i in range(db.memtable_items_limit):
            db.set(f"{i}", f"{i*3}")

        # one pass i*4 -> file3
        for i in range(db.memtable_items_limit):
            db.set(f"{i}", f"{i*4}")

        # then check that the value returned if for i*4
        for i in range(db.memtable_items_limit):
            assert db.get(f"{i}") == f"{i*4}"

    def test_clear_sstables(self, db):
        for i in range(db.memtable_items_limit + 1):
            db.set(f"{i}", f"{i*2}")
        db.clear()
        assert db.get("1", None) is None
        assert db.is_empty()

    def test_merge_ssfiles_on_restore(self, SoonerDB, tmpdir):
        memtable_items_limit = 10
        db = SoonerDB(tmpdir, memtable_items_limit=memtable_items_limit)

        # -> file1
        for i in [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]:
            db.set(f"{i:.>2}", '*')

        # -> file2
        for i in [3, 5, 7, 9, 11, 13, 15, 17, 19, 21]:
            db.set(f"{i:.>2}", '~')

        # -> file3
        for i in [1, 4, 7, 10, 13, 16, 19, 22, 25, 26]:
            db.set(f"{i:.>2}", '+')

        assert len(db._sstables) == 3

        del db

        db = SoonerDB(tmpdir)

        assert len(db._sstables) == 1

        assert list(db) == [
            ('.1', '+'),
            ('.2', '*'),
            ('.3', '~'),
            ('.4', '+'),
            ('.5', '~'),
            ('.6', '*'),
            ('.7', '+'),
            ('.8', '*'),
            ('.9', '~'),
            ('10', '+'),
            ('11', '~'),
            ('12', '*'),
            ('13', '+'),
            ('14', '*'),
            ('15', '~'),
            ('16', '+'),
            ('17', '~'),
            ('18', '*'),
            ('19', '+'),
            ('20', '*'),
            ('21', '~'),
            ('22', '+'),
            ('25', '+'),
            ('26', '+'),
        ]
