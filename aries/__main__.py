from itertools import cycle
from string import ascii_uppercase
from prettytable import PrettyTable
import dataclasses
from typing import Any, Optional
from collections import OrderedDict


@dataclasses.dataclass
class Entry:
    key: str
    val: Any


class DiskPage:
    def __init__(self, page_id: int):
        self._entries: list[Entry] = []
        self.pageLSN = -1
        self.page_ID = page_id

    def has_key(self, key: str) -> bool:
        for e in self._entries:
            if e.key == key:
                return True
        return False


class Disk:
    def __init__(self):
        num_pages = 4
        entries_per_page = 4
        total_entries = num_pages * entries_per_page

        # populate pages with default values
        pages = [DiskPage(id_) for id_ in range(num_pages)]
        gen_key = iter(ascii_uppercase)

        for page_id in range(num_pages):
            for _ in range(entries_per_page):
                key = next(gen_key)
                entry = Entry(key, 0)
                pages[page_id]._entries.append(entry)
        self._pages = pages

    def get_page(self, page_ID: int):
        return self._pages[page_ID]

    def __repr__(self):
        tbl = PrettyTable()
        tbl.field_names = ["Page", "Key", "Val"]
        for p in self._pages:
            tbl.add_row([f"Page {p.page_ID}", "", ""])
            for e in p._entries:
                tbl.add_row(["", e.key, e.val])
        tbl.title = "Disk"
        tbl.align = "l"
        tbl.align["Val"] = "r"
        return str(tbl)


class BufferPage:
    def __init__(self, disk_page: DiskPage):
        self.is_dirty = False
        self.page_ID = disk_page.page_ID
        # create copy of entries to simulate loading from disk to memory
        # NOTE: dataclasses.replace does a shallow copy, so ensure all the entry
        # fields are not deep
        self._entries = [dataclasses.replace(e) for e in disk_page._entries]


class BufferPool:
    def __init__(self, capacity: int, disk: Disk):
        # gen mapping from entry key to disk page ID
        keys_to_page_ID: dict[str, int] = dict()
        for p in disk._pages:
            for e in p._entries:
                keys_to_page_ID[e.key] = p.page_ID

        self._disk = disk
        self.keys_to_page_ID = keys_to_page_ID
        self.pool: list[Optional[BufferPage]] = [None] * capacity

    def get_page(self, page_ID: int) -> BufferPage:
        for bp in self.pool:
            if bp is not None and bp.page_ID == page_ID:
                # cache hit
                return bp
        # cache miss
        bp = self._load_page_from_disk(page_ID)

    def _load_page_from_disk(self, page_ID) -> BufferPage:
        # find empty slot in pool
        slot = -1
        for i, bp in enumerate(self.pool):
            if bp is None:
                slot = i
                break

        # if there isn't any empty slot, evict a buffer page from the pool
        if slot == -1:
            slot = self._evict_page()

        # load page from disk

        # add page to buffer pool

    def _evict_page(self) -> int:
        """
        Evicts a buffer page from the pool given a policy (such as LRU) and
        returns its previous slot.
        If the buffer page is dirty, it is first flushed to disk before eviction
        """
        page_id = -1
        slot = -1
        self._flush_page(page_id)
        return slot

    def _flush_page(self, slot: int):
        pass


class Database:
    def __init__(self):
        self.disk = Disk()
        buffer_pool_capacity = 2
        self.buffer_pool = BufferPool(buffer_pool_capacity, self.disk)
        print(self.disk)

    def read_entry(self, key: str) -> Entry:
        return None


def main():
    db = Database()
    return


if __name__ == "__main__":
    main()
