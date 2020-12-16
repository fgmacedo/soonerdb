from sortedcontainers import SortedDict
from pathlib import Path
from struct import pack, unpack


def write_pair(buffer_write, key, value):
    key_bytes = key.encode('utf-8')
    value_bytes = value.encode('utf-8')
    buffer_write(pack("I", len(key_bytes)))
    buffer_write(key_bytes)
    buffer_write(pack("I", len(value_bytes)))
    buffer_write(value_bytes)


def read_pairs(buffer_read):
    while key_len_bytes := buffer_read(4):
        (key_len, ) = unpack('I', key_len_bytes)
        key_bytes = buffer_read(key_len)
        (value_len, ) = unpack('I', buffer_read(4))
        value_bytes = buffer_read(value_len)
        yield key_bytes.decode('utf-8'), value_bytes.decode('utf-8')


class WAL:
    """
    Write-ahead logging.
    """

    def __init__(self, path):
        self.path = path
        self.wal = open(path, 'ab+')

    def __del__(self):
        self.wal.close()

    def put(self, key, value):
        write_pair(self.wal.write, key, value)
        self.wal.flush()

    def restore(self, memtable):
        self.wal.seek(0)
        for key, value in read_pairs(self.wal.read):
            memtable[key] = value


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
