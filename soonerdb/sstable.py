from pathlib import Path

from .memtable import MemTable
from .io import write_pair, read_pairs, write_index, read_index


class SSTable:

    def __init__(self, path, memtable: MemTable = None):
        self.path = Path(path)
        self.index_path = path.with_name(path.name + '.index')
        self.search_index = MemTable()

        if self.path.exists() and memtable is not None:
            raise ValueError(f"SSTables are immutable ({self}).")
        elif self.path.exists():
            self._load_search_index_from_file()
        elif memtable is not None:
            self._write_sstable_from_memtable(memtable)
        else:
            raise ValueError(f"Cannot initialize SSTable file {self.path}.")

    def __repr__(self):
        return f'{self.__class__.__name__}("{self.path}")'

    def __str__(self):
        return str(self.path)

    def _load_search_index_from_file(self):
        memtable = self.search_index
        with open(self.index_path, 'rb') as index_file:
            read_fn = index_file.read
            index_file.seek(0)
            for key, value in read_index(read_fn):
                memtable[key] = value

    def _write_sstable_from_memtable(self, memtable: MemTable):
        search_index = self.search_index

        with open(self.path, 'ab+') as ss_file:
            write_fn = ss_file.write
            position_fn = ss_file.tell
            for key, value in memtable.items():
                position = position_fn()
                write_pair(write_fn, key, value)
                search_index[key] = position

        with open(self.index_path, 'ab+') as index_file:
            write_fn = index_file.write
            for key, position in search_index.items():
                write_index(write_fn, key, position)

    def get(self, key, default=None):
        position = self.search_index.get(key, None)
        if position is None:
            return default
        with open(self.path, 'rb') as f:
            f.seek(position)
            read_fn = read_pairs(f.read)
            _, value = next(read_fn)
            return value
