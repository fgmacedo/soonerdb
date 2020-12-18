from collections import deque
from typing import Iterable, Tuple
from threading import Thread, Event
from weakref import WeakMethod


def merge_iterables(*iterables: Iterable[Tuple[str, str]]):
    """
    Read each iterable side-by-side, look at the first key on each file,
    copy the lowest key, and repeat. Discart equals keys.

    This produces a new merged iterable, also sorted by key.
    """

    tables = [iter(sst) for sst in iterables]
    heads = deque()
    for table in tables[:]:
        try:
            heads.append(next(table))
        except StopIteration:
            tables.remove(table)

    lowest_key, lowest_value = None, None

    def get_key(item):
        return item[1][0]

    def pick_lowest():
        idx, (lowest_key, lowest_value, ) = sorted(enumerate(heads), key=get_key)[0]
        try:
            next_pair = next(tables[idx])
            heads[idx] = next_pair
        except StopIteration:
            del tables[idx]
            del heads[idx]
        return lowest_key, lowest_value

    def remove_lte():
        indexex_to_pop = [
            idx for idx, (k, v) in enumerate(heads)
            if lowest_key is not None and k <= lowest_key
        ]
        for idx in reversed(indexex_to_pop):
            try:
                next_pair = next(tables[idx])
                heads[idx] = next_pair
            except StopIteration:
                del tables[idx]
                del heads[idx]
        return bool(indexex_to_pop)

    while True:
        if not tables and not heads:
            break

        if remove_lte():
            continue

        lowest_key, lowest_value = pick_lowest()
        yield lowest_key, lowest_value


class MergeThread(Thread):
    def __init__(self, merge_fn, sleep_interval=5):
        super().__init__(None, daemon=True)
        self._kill = Event()
        self._interval = sleep_interval

        # Weak ref to avoid keeping the SoonerDB instance in memory due to circular references.
        self._ref_to_fn = WeakMethod(merge_fn)

    def run(self):
        while True:
            still_alive = self._run_target()
            if not still_alive:
                break

            # Sleep and watch for the kill event
            is_killed = self._kill.wait(self._interval)
            if is_killed:
                break

    def _run_target(self):
        method = self._ref_to_fn()
        if method is None:
            return False

        method()
        return True

    def kill(self):
        self._kill.set()
