from __future__ import annotations

import time
from collections import defaultdict, deque
from threading import Lock


class SlidingWindowRateLimiter:
    def __init__(self, max_requests: int, window_seconds: float):
        self.max_requests = max(1, max_requests)
        self.window_seconds = max(1.0, window_seconds)
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = Lock()

    def check(self, key: str) -> tuple[bool, float]:
        now = time.monotonic()
        with self._lock:
            bucket = self._events[key]
            cutoff = now - self.window_seconds
            while bucket and bucket[0] <= cutoff:
                bucket.popleft()

            if len(bucket) >= self.max_requests:
                retry_after = max(0.0, self.window_seconds - (now - bucket[0]))
                return False, retry_after

            bucket.append(now)
            return True, 0.0
