from pathlib import Path

import pytest


class TestSSTable:

    def test_index(self, lor_sstable):
        expected_index = [('Aragorn', 0), ('Frodo', 20), ('Gandalf', 39), ('Legolas', 60)]
        assert list(lor_sstable.search_index.items()) == expected_index

    def test_get(self, lor_sstable):
        assert lor_sstable.get('Frodo') == "Hobbit"
        assert lor_sstable.get('Legolas') == "Elf"
        assert lor_sstable.get('Elrond') is None

    def test_file_contents(self, lor_sstable):
        assert lor_sstable.path.read_bytes() == (
            b'\x07\x00\x00\x00Aragorn\x05\x00\x00\x00Human\x05\x00\x00\x00Frod'
            b'o\x06\x00\x00\x00Hobbit\x07\x00\x00\x00Gandalf\x06\x00\x00\x00Wizard'
            b'\x07\x00\x00\x00Legolas\x03\x00\x00\x00Elf'
        )

    def test_index_contents(self, lor_sstable):
        assert lor_sstable.index_path.read_bytes() == (
            b'\x07\x00\x00\x00Aragorn\x00\x00\x00\x00\x05\x00\x00\x00Frodo\x14\x00\x00\x00'
            b"\x07\x00\x00\x00Gandalf'\x00\x00\x00\x07\x00\x00\x00Legolas<\x00\x00\x00"
        )

    def test_cannot_override_an_sstable(self, tmpdir, lor_memtable, SSTable):
        path = Path(tmpdir / 'table1.dat')
        table = SSTable(path, lor_memtable)

        del table

        with pytest.raises(ValueError) as e:
            SSTable(path, lor_memtable)

        assert e.match("immutable")

    def test_cannot_initialize_with_missing_file(self, tmpdir, SSTable):
        path = Path(tmpdir / 'table1.dat')

        with pytest.raises(ValueError) as e:
            SSTable(path)

        assert e.match("initialize")

    def test_repr(self, lor_sstable):
        assert repr(lor_sstable) == f'SSTable("{lor_sstable.path}")'
