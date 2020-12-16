from datetime import datetime
from pathlib import Path
from threading import Lock

from .memtable import MemTable, SortedKeyList
from .wal import WAL
from .sstable import SSTable

write_mutex = Lock()


class SoonerDB:

    def __init__(self, path, memtable_items_limit=512):
        self.path = Path(path)
        self.memtable_items_limit = memtable_items_limit
        self._memtable = MemTable()
        self._sstables = SortedKeyList(key=lambda x: str(x.path))
        self._wal = WAL(self.path / 'wal.dat')
        self._wal.restore(self._memtable)
        self._load_sstables()

    def get(self, key, default=None):
        value = self._memtable.get(key, default)
        if value is not None:
            return value

        for table in reversed(self._sstables):
            value = table.get(key)
            if value is not None:
                return value

        return default

    def set(self, key, value):
        with write_mutex:
            self._wal.set(key, value)
            self._memtable[key] = value
            if self._memtable_limit_reached():
                self._dump_to_sstable()

    def delete(self, key):
        del self._memtable[key]

    def is_empty(self):
        return len(self._memtable) == 0

    def clear(self):
        self._memtable.clear()

    def items(self):
        return self._memtable.items()

    def _memtable_limit_reached(self):
        return len(self._memtable) >= self.memtable_items_limit

    def _dump_to_sstable(self):
        # dump memtable to sstable
        # add sstable to the list
        # clear memtable and wal log
        when = datetime.utcnow().isoformat().replace(':', '_')
        sstable_file = self.path / f'sst_{when}.dat'
        self._sstables.add(SSTable(sstable_file, self._memtable))
        self._memtable = MemTable()
        self._wal.clear()

    def _load_sstables(self):
        for sstable_file in self.path.glob('sst_*.dat'):
            self._sstables.add(SSTable(sstable_file))
