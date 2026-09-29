import time
import threading
from typing import Tuple, Dict, List
from collections import defaultdict

class SlidingWindowRateLimiter:
    """
    High-performance, thread-safe in-memory sliding-window rate limiter.
    Provides OWASP API4:2023 protection against endpoint enumeration and abuse.
    """
    def __init__(self, cleanup_interval_seconds: int = 300):
        self._lock = threading.Lock()
        self._records: Dict[str, List[float]] = defaultdict(list)
        self._last_cleanup = time.time()
        self._cleanup_interval = cleanup_interval_seconds

    def is_rate_limited(self, key: str, max_requests: int, window_seconds: int) -> Tuple[bool, int]:
        """
        Evaluates whether a request for a given key exceeds max_requests within window_seconds.
        Returns:
            (is_limited: bool, retry_after_seconds: int)
        """
        now = time.time()
        cutoff = now - window_seconds

        with self._lock:
            # Periodic cleanup of expired records
            if now - self._last_cleanup > self._cleanup_interval:
                self._cleanup_stale_records(now)

            timestamps = self._records[key]
            # Filter out entries older than the sliding window
            valid_timestamps = [ts for ts in timestamps if ts > cutoff]
            self._records[key] = valid_timestamps

            if len(valid_timestamps) >= max_requests:
                # Oldest valid timestamp in window dictates when a slot frees up
                oldest_in_window = valid_timestamps[0]
                retry_after = max(1, int(window_seconds - (now - oldest_in_window)))
                return True, retry_after

            # Record this request
            self._records[key].append(now)
            return False, 0

    def reset_key(self, key: str):
        """Manually resets a rate limit key (e.g. upon successful authentication)."""
        with self._lock:
            if key in self._records:
                del self._records[key]

    def _cleanup_stale_records(self, now: float):
        """Cleans up keys where all timestamps are older than 1 hour."""
        cutoff = now - 3600
        keys_to_delete = []
        for k, ts_list in self._records.items():
            if not ts_list or ts_list[-1] < cutoff:
                keys_to_delete.append(k)
        for k in keys_to_delete:
            del self._records[k]
        self._last_cleanup = now


# Global singleton instance
quotation_rate_limiter = SlidingWindowRateLimiter()
