from sortedcontainers import SortedDict


class SoonerDB:

    def __init__(self, path):
        self.path = path
        self.memtable = SortedDict()

    def get(self, key, default=None):
        return self.memtable.get(key, default)

    def put(self, key, value):
        self.memtable[key] = value

    def delete(self, key):
        del self.memtable[key]

    def is_empty(self):
        return len(self.memtable) == 0

    def clear(self):
        self.memtable.clear()

    def items(self):
        return self.memtable.items()
