from pathlib import Path

import pytest


@pytest.fixture
def db(tmpdir):
    from soonerdb import SoonerDB
    return SoonerDB(tmpdir)


@pytest.fixture
def SoonerDB(tmpdir):
    from soonerdb import SoonerDB
    return SoonerDB


@pytest.fixture
def SSTable(tmpdir):
    from soonerdb.sstable import SSTable
    return SSTable


@pytest.fixture
def MemTable():
    from soonerdb.soonerdb import MemTable
    return MemTable


@pytest.fixture
def memtable(MemTable):
    return MemTable()


@pytest.fixture
def lor_memtable(memtable):
    memtable["Frodo"] = "Hobbit"
    memtable["Gandalf"] = "Wizard"
    memtable["Legolas"] = "Elf"
    memtable["Aragorn"] = "Human"
    return memtable


@pytest.fixture
def lor_sstable(tmpdir, lor_memtable, SSTable):
    path = Path(tmpdir / 'table1.dat')
    return SSTable(path, lor_memtable)
