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


class TestMerge:

    def test_merge_exactly_same(self, tmpdir, lor_memtable, SSTable):
        table1 = SSTable(Path(tmpdir / 'table1.dat'), lor_memtable)
        table2 = SSTable(Path(tmpdir / 'table2.dat'), lor_memtable)

        merged = table1.merge(table2)

        assert list(merged.items()) == list(table1)

    def test_merge_three_tables(self, tmpdir, MemTable, SSTable):
        table1 = SSTable(
            Path(tmpdir / 'table1.dat'),
            MemTable([
                ("a", "asno"),
                ("c", "cavalo"),
                ("e", "elefante"),
                ("g", "gato"),
                ("i", "iguana"),
                ("k", "koala"),
                ("m", "macaco"),
            ])
        )
        table2 = SSTable(
            Path(tmpdir / 'table2.dat'),
            MemTable([
                ("a", "avelâ"),
                ("d", "damasco"),
                ("g", "gengibre"),
                ("j", "jiló"),
                ("m", "maçã"),
                ("p", "pitaia"),
                ("s", "sagu"),
                ("u", "uva"),
                ("y", "Yuzu"),
            ])
        )
        table3 = SSTable(
            Path(tmpdir / 'table3.dat'),
            MemTable([
                ("b", "bola"),
                ("e", "escorregador"),
                ("h", "helicóptero"),
                ("k", "kapla"),
                ("n", "navio"),
                ("q", "quebra-cabeça"),
                ("t", "trem"),
            ])
        )

        merged = table1.merge(table2, table3)

        assert list(merged.items()) == [
            ("a", "asno"),
            ("b", "bola"),
            ("c", "cavalo"),
            ("d", "damasco"),
            ("e", "elefante"),
            ("g", "gato"),
            ("h", "helicóptero"),
            ("i", "iguana"),
            ("j", "jiló"),
            ("k", "koala"),
            ("m", "macaco"),
            ("n", "navio"),
            ("p", "pitaia"),
            ("q", "quebra-cabeça"),
            ("s", "sagu"),
            ("t", "trem"),
            ("u", "uva"),
            ("y", "Yuzu"),
        ]
