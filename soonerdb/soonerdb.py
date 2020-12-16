from pathlib import Path
from sortedcontainers import SortedDict

from .wal import WAL


class SoonerDB:

    def __init__(self, path):
        self.path = Path(path)
        self.memtable = SortedDict()
        self.wal = WAL(self.path / 'wal.dat')
        self.wal.restore(self.memtable)

    def get(self, key, default=None):
        return self.memtable.get(key, default)

    def put(self, key, value):
        self.wal.put(key, value)
        self.memtable[key] = value

    def delete(self, key):
        del self.memtable[key]

    def is_empty(self):
        return len(self.memtable) == 0

    def clear(self):
        self.memtable.clear()

    def items(self):
        return self.memtable.items()
