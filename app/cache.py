from __future__ import annotations

import time
from collections import OrderedDict
from threading import Lock
from typing import Generic, TypeVar

T = TypeVar("T")


class TTLCache(Generic[T]):
    def __init__(self, maxsize: int, ttl_seconds: float):
        self.maxsize = max(1, maxsize)
        self.ttl_seconds = max(0.001, ttl_seconds)
        self._items: OrderedDict[object, tuple[float, T]] = OrderedDict()
        self._lock = Lock()

    def get(self, key: object) -> T | None:
        now = time.monotonic()
        with self._lock:
            self._purge_expired(now)
            record = self._items.get(key)
            if record is None:
                return None
            expires_at, value = record
            if expires_at <= now:
                self._items.pop(key, None)
                return None
            self._items.move_to_end(key)
            return value

    def set(self, key: object, value: T) -> None:
        now = time.monotonic()
        with self._lock:
            self._purge_expired(now)
            self._items[key] = (now + self.ttl_seconds, value)
            self._items.move_to_end(key)
            while len(self._items) > self.maxsize:
                self._items.popitem(last=False)

    def _purge_expired(self, now: float) -> None:
        expired_keys = [
            key
            for key, (expires_at, _) in self._items.items()
            if expires_at <= now
        ]
        for key in expired_keys:
            self._items.pop(key, None)
