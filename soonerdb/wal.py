from .io import write_pair, read_pairs


class WAL:
    """
    Write-ahead logging.
    """

    def __init__(self, path):
        self.path = path
        self.wal = open(path, 'ab+')

    def __del__(self):
        self.wal.close()

    def set(self, key, value):
        write_pair(self.wal.write, key, value)
        self.wal.flush()

    def restore(self, memtable):
        self.wal.seek(0)
        read_fn = self.wal.read
        for key, value in read_pairs(read_fn):
            memtable[key] = value

    def clear(self):
        self.wal.truncate(0)
