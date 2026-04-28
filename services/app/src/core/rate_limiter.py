"""Synchronous token-bucket rate limiter.

Use for strict API quotas (e.g., Polygon Basic at 5 req/min). The acquire()
call blocks until a token is available, ensuring we never exceed the quota.
"""
from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)


class TokenBucketRateLimiter:
    """Token-bucket: refills at `rate_per_minute / 60` tokens/sec, capacity = rate_per_minute.

    Thread-safe via internal lock. Synchronous API: acquire() blocks until a
    token is available.
    """

    def __init__(self, rate_per_minute: int, name: str = "limiter"):
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be > 0")
        self.rate_per_second = rate_per_minute / 60.0
        self.capacity = float(rate_per_minute)
        self._tokens = float(rate_per_minute)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()
        self._name = name

    def acquire(self) -> None:
        with self._lock:
            self._refill()
            while self._tokens < 1.0:
                deficit = 1.0 - self._tokens
                wait = deficit / self.rate_per_second
                logger.debug("[%s] rate-limit sleep %.2fs", self._name, wait)
                # Release lock during sleep so other threads can refill.
                self._lock.release()
                try:
                    time.sleep(wait + 0.05)
                finally:
                    self._lock.acquire()
                self._refill()
            self._tokens -= 1.0

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.capacity, self._tokens + elapsed * self.rate_per_second)
        self._last_refill = now
