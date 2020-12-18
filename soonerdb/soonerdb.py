from pathlib import Path
from threading import Lock

from .memtable import MemTable, SortedKeyList
from .sstable import SSTable
from .wal import WAL
from .merge import MergeThread

write_mutex = Lock()
_sentinel = object()


class SoonerDB:

    def __init__(self, path, memtable_items_limit=512):
        self.path = Path(path)
        if not self.path.exists():
            self.path.mkdir(parents=True, exist_ok=True)
        self.memtable_items_limit = memtable_items_limit
        self._memtable = MemTable()
        self._sstables = SortedKeyList(key=lambda x: str(x.path))
        self._wal = WAL(self.path / 'wal.dat')
        self._wal.restore(self._memtable)
        self._load_sstables()
        self._merge_sstables()
        self._merge_daemon = MergeThread(
            merge_fn=self._merge_sstables,
            sleep_interval=5,
        )
        self._merge_daemon.start()

    def __iter__(self):
        memtable = SSTable.merge_all(
            self._memtable.items(),
            *reversed(self._sstables),
        )
        return iter(memtable.items())

    def __getitem__(self, key):
        return self.get(key)

    def __setitem__(self, key, value):
        return self.set(key, value)

    def __delitem__(self, key):
        self.delete(key)

    def __contains__(self, key):
        """
        Only for convenience, as there's no performance gain over `.get()`
        """
        sentinel = object()
        return self.get(key, sentinel) is not sentinel

    def __del__(self):
        self._merge_daemon.kill()
        self._merge_daemon.join()

    def get(self, key, default=_sentinel):
        value = self._memtable.get(key, _sentinel)
        if value is not _sentinel:
            return value

        for table in reversed(self._sstables):
            value = table.get(key, _sentinel)
            if value is not _sentinel:
                return value

        if default is _sentinel:
            raise KeyError(f"Key '{key}' not found.")

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
        old_tables = self._sstables[:]
        with write_mutex:
            self._memtable.clear()
            self._wal.clear()
            self._sstables.clear()
        for table in old_tables:
            table.delete()

    def items(self):
        return iter(self)

    def _memtable_limit_reached(self):
        return len(self._memtable) >= self.memtable_items_limit

    def _dump_to_sstable(self):
        # dump memtable to sstable
        # add sstable to the list
        # clear memtable and wal log
        self._sstables.add(SSTable(self.path, self._memtable))
        self._memtable = MemTable()
        self._wal.clear()

    def _load_sstables(self):
        for sstable_file in self.path.glob('sst_*.dat'):
            self._sstables.add(SSTable(sstable_file))

    def _merge_sstables(self):
        if len(self._sstables) < 2:
            return

        # all merge can be done without locking
        old_tables = self._sstables[:]
        memtable = SSTable.merge_all(*reversed(self._sstables))
        merged_sstable = SSTable(self.path, memtable)

        with write_mutex:
            self._sstables.add(merged_sstable)
            for table in old_tables:
                self._sstables.remove(table)

        for table in old_tables:
            table.delete()
