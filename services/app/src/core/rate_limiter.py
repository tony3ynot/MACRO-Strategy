"""Synchronous strict-interval rate limiter.

Why strict-interval (not token-bucket):
  Polygon Basic enforces 5 req/min as a SLIDING window, not a fixed
  per-minute counter. A traditional token bucket allows bursting (5 calls
  back-to-back, then wait), which trips the sliding-window detector and
  yields 429s. We instead enforce a hard minimum interval between calls
  (60s / rate_per_minute × safety_margin), which guarantees the rolling
  window stays under the cap.

Acquire() blocks the caller until enough time has elapsed since the
previous call. Thread-safe via internal lock.
"""
from __future__ import annotations

import logging
import threading
import time

logger = logging.getLogger(__name__)

DEFAULT_SAFETY_MARGIN = 1.08  # 8% buffer over nominal interval


class TokenBucketRateLimiter:
    """Strict-interval limiter (name retained for compatibility).

    `rate_per_minute` is the API's stated cap; we space calls by
    (60 / rate_per_minute) × safety_margin seconds. Setting safety_margin
    above 1.0 protects against sliding-window edge cases where a call
    timed exactly at the boundary still counts as the (N+1)-th call.
    """

    def __init__(
        self,
        rate_per_minute: int,
        name: str = "limiter",
        safety_margin: float = DEFAULT_SAFETY_MARGIN,
    ):
        if rate_per_minute <= 0:
            raise ValueError("rate_per_minute must be > 0")
        if safety_margin < 1.0:
            raise ValueError("safety_margin must be >= 1.0 (otherwise we'd burst)")
        self.min_interval = (60.0 / rate_per_minute) * safety_margin
        self._last_call = 0.0  # monotonic clock; 0.0 means "first call OK immediately"
        self._lock = threading.Lock()
        self._name = name

    def acquire(self) -> None:
        with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_call
            if self._last_call > 0 and elapsed < self.min_interval:
                wait = self.min_interval - elapsed
                logger.debug("[%s] interval sleep %.2fs", self._name, wait)
                self._lock.release()
                try:
                    time.sleep(wait)
                finally:
                    self._lock.acquire()
            self._last_call = time.monotonic()
