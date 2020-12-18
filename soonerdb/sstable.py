from datetime import datetime
from pathlib import Path

from .io import write_pair, read_pairs, write_index, read_index
from .memtable import MemTable
from .merge import merge_iterables


class SSTable:

    def __init__(self, path, memtable: MemTable = None):
        self.path = Path(path)
        if self.path.is_dir():
            when = datetime.utcnow().isoformat().replace(':', '_')
            self.path = self.path / f'sst_{when}.dat'

        self.index_path = self.path.with_name(self.path.name + '.index')
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

    def __iter__(self):
        with open(self.path, 'rb') as f:
            for key, value in read_pairs(f.read):
                yield key, value

    def delete(self):
        self.path.unlink()
        self.index_path.unlink()

    @staticmethod
    def merge_all(*sstables):
        """
        Merge tables. We start from most recent.

        Read each file side-by-side, look at the first key on each file, copy the lowest key,
        and repeat. Discart equals keys.

        This produces a new merged segment file, also sorted by key.
        """
        memtable = MemTable()
        for key, value in merge_iterables(*sstables):
            memtable[key] = value

        return memtable

    def merge(self, *others):
        return self.merge_all(self, *others)
