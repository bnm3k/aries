from string import ascii_uppercase
from prettytable import PrettyTable
import dataclasses
from typing import Any, Optional
from collections import deque
from collections.abc import Iterator
from copy import deepcopy


@dataclasses.dataclass
class Entry:
    key: str
    val: Any


class DiskPage:
    def __init__(self, page_ID: int):
        self.page_ID = page_ID
        self.page_LSN = -1
        self.entries: list[Entry] = []


class Disk:
    def __init__(self, max_pages: int):
        self._pages = [DiskPage(id_) for id_ in range(max_pages)]

    @classmethod
    def init_with_entries(
        cls,
        num_pages: int,
        entries_per_page: int,
        keys: Iterator[str],
        default_val: Any = None,
    ):
        disk = cls(num_pages)
        if default_val is None:
            default_val = 0
        for page_id in range(num_pages):
            entries = [Entry(next(keys), default_val) for _ in range(entries_per_page)]
            disk._pages[page_id].entries = entries
        return disk

    def read_page(self, page_ID: int) -> DiskPage:
        d_page = self._pages[page_ID]
        return deepcopy(d_page)

    def write_page(self, d_page: DiskPage):
        page_ID = d_page.page_ID
        self._pages[page_ID] = deepcopy(d_page)

    def __str__(self):
        tbl = PrettyTable()
        tbl.field_names = ["Page", "Key", "Val"]
        for p in self._pages:
            tbl.add_row([f"Page {p.page_ID}", "", ""])
            for e in p.entries:
                tbl.add_row(["", e.key, e.val])
        tbl.title = "Disk"
        tbl.align = "l"
        tbl.align["Val"] = "r"
        return str(tbl)


class LRUPolicy:
    def __init__(self):
        self._queue = deque()

    def add(self, slot: int):
        self._queue.appendleft(slot)

    def mark_usage(self, slot: int):
        """
        TODO: use faster data structure, e.g. dict + double-linked list
        raises ValueError if slot not previously inserted
        """
        self._queue.remove(slot)
        self._queue.insert(0, slot)

    def pop_eviction_candidate(self) -> int:
        """
        raises IndexError if empty
        """
        return self._queue.pop()

    def __str__(self):
        as_str = f"[MRU {', '.join(str(n) for n in self._queue)} LRU]"
        return as_str


class Page:
    def __init__(self, disk_page: DiskPage):
        self.page_ID = disk_page.page_ID
        self.pageLSN = disk_page.page_LSN
        self.entries = disk_page.entries
        self.is_dirty = False

    def to_disk_page(self) -> DiskPage:
        p = DiskPage(self.page_ID)
        p.page_LSN = self.pageLSN
        p.entries = self.entries
        return p


class BufferPool:
    def __init__(self, capacity: int, disk: Disk):
        self._disk = disk
        self._slots: list[Optional[Page]] = [None] * capacity
        self._replacement_policy = LRUPolicy()

    def get_page(self, page_ID: int) -> Page:
        for slot, page in enumerate(self._slots):
            if page is not None and page.page_ID == page_ID:
                # cache hit
                self._replacement_policy.mark_usage(slot)
                return page

        # cache miss
        # find slot to place page in
        slot = -1
        for i, bp in enumerate(self._slots):
            if bp is None:
                slot = i
                break

        # if there isn't any empty slot, evict a buffer page from the pool
        if slot == -1:
            slot = self._replacement_policy.pop_eviction_candidate()
            prev_page = self._slots[slot]
            if prev_page is None:
                raise Exception("page to be evicted should not be None")
            if prev_page.is_dirty:
                self._disk.write_page(prev_page.to_disk_page())

        # read page from disk and add to buffer pool
        page = Page(self._disk.read_page(page_ID))
        self._slots[slot] = page
        self._replacement_policy.add(slot)
        return page

    def __str__(self):
        tbl = PrettyTable()
        tbl.field_names = ["Slot", "Key", "Val"]
        for i, p in enumerate(self._slots):
            if p is None:
                tbl.add_row([f"[{i}] slot empty", "", ""])
                continue
            dirty = "F"
            if p.is_dirty:
                dirty = "T"
            tbl.add_row([f"[{i}] Page {p.page_ID} (Dirty={dirty})", "", ""])
            for e in p.entries:
                tbl.add_row(["", e.key, e.val])
        tbl.title = f"Buffer Pool: {self._replacement_policy}"
        tbl.align = "l"
        tbl.align["Val"] = "r"
        return str(tbl)


class Database:
    def __init__(self, disk: Disk, buffer_pool_capacity: int):
        # gen mapping from entry key to disk page ID
        keys_to_page_ID: dict[str, int] = dict()
        for p in disk._pages:
            for e in p.entries:
                keys_to_page_ID[e.key] = p.page_ID

        self.disk = disk
        self._keys_to_page_ID = keys_to_page_ID
        self.buffer_pool = BufferPool(buffer_pool_capacity, self.disk)

    def get_page_ID(self, key: str) -> Optional[int]:
        return self._keys_to_page_ID.get(key)

    def get_page(self, page_ID: int) -> Page:
        return self.buffer_pool.get_page(page_ID)


def main():
    # set up disk with default vals
    num_pages = 8
    entries_per_page = 2
    keys = iter(ascii_uppercase)

    disk = Disk.init_with_entries(num_pages, entries_per_page, keys)
    DB = Database(disk, buffer_pool_capacity=4)

    for page_ID in range(num_pages):
        _ = DB.get_page(page_ID)
    print(DB.buffer_pool)

    return


if __name__ == "__main__":
    main()
