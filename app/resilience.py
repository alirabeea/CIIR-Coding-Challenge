from __future__ import annotations

import time
from dataclasses import dataclass
from threading import Lock


@dataclass
class _BreakerState:
    failures: int = 0
    opened_at: float | None = None


class CircuitBreaker:
    def __init__(self, failure_threshold: int, reset_seconds: float):
        self.failure_threshold = max(1, failure_threshold)
        self.reset_seconds = max(1.0, reset_seconds)
        self._states: dict[str, _BreakerState] = {}
        self._lock = Lock()

    def allow(self, name: str) -> bool:
        now = time.monotonic()
        with self._lock:
            state = self._states.get(name)
            if state is None or state.opened_at is None:
                return True
            if now - state.opened_at >= self.reset_seconds:
                self._states[name] = _BreakerState()
                return True
            return False

    def record_success(self, name: str) -> None:
        with self._lock:
            self._states[name] = _BreakerState()

    def record_failure(self, name: str) -> None:
        now = time.monotonic()
        with self._lock:
            state = self._states.setdefault(name, _BreakerState())
            state.failures += 1
            if state.failures >= self.failure_threshold:
                state.opened_at = now
