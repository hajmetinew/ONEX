"""Low-overhead per-link bandwidth limiter used by ONEX relay backends.

Uses a small token-bucket per UUID instead of sleeping for every received
chunk.  The bucket is shared by upload/download directions for a link, so the
configured limit remains an aggregate per-link rate.
"""
from __future__ import annotations

import asyncio
import time

MIN_RATE = 1024
MIN_BURST = 16 * 1024
MAX_BURST = 4 * 1024 * 1024

_buckets: dict[str, "_Bucket"] = {}
_buckets_lock = asyncio.Lock()
_last_prune = 0.0
PRUNE_INTERVAL = 300.0
STALE_BUCKET_AGE = 3600.0


class _Bucket:
    __slots__ = ("rate", "capacity", "tokens", "last", "lock", "touched")


    def __init__(self, rate: int):
        self.rate = max(int(rate), MIN_RATE)
        self.capacity = min(MAX_BURST, max(self.rate, MIN_BURST))
        self.tokens = float(self.capacity)
        self.last = time.monotonic()
        self.lock = asyncio.Lock()
        self.touched = time.monotonic()

    def refill(self, now: float) -> None:
        elapsed = now - self.last
        if elapsed > 0:
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)

    async def consume(self, amount: int) -> None:
        while True:
            async with self.lock:
                now = time.monotonic()
                self.refill(now)
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                deficit = amount - self.tokens
                wait = max(0.004, min(0.5, deficit / self.rate))
            await asyncio.sleep(wait)


def _rate(uid: str) -> int:
    try:
        from main import LINKS
        link = LINKS.get(uid) or {}
        return max(0, int(link.get("speed_limit_bytes", 0) or 0))
    except Exception:
        return 0


async def throttle(uid: str, amount: int) -> None:
    """Apply an aggregate per-link byte-rate limit without blocking the loop."""
    if amount <= 0:
        return
    rate = _rate(uid)
    if rate <= 0:
        return
    global _last_prune
    async with _buckets_lock:
        now = time.monotonic()
        if now - _last_prune >= PRUNE_INTERVAL:
            try:
                from main import LINKS
                active = set(LINKS.keys())
                for stale_uid, stale_bucket in list(_buckets.items()):
                    if stale_uid not in active or now - stale_bucket.touched > STALE_BUCKET_AGE:
                        _buckets.pop(stale_uid, None)
            except Exception:
                pass
            _last_prune = now
        bucket = _buckets.get(uid)
        wanted_rate = max(rate, MIN_RATE)
        if bucket is None or bucket.rate != wanted_rate:
            bucket = _Bucket(rate)
            _buckets[uid] = bucket
        bucket.touched = now
    await bucket.consume(amount)


def reset_bucket(uid: str) -> None:
    """Drop a limiter state when a link is removed or its limit is changed."""
    _buckets.pop(uid, None)
