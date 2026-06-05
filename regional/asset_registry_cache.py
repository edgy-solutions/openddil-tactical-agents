"""In-memory asset_id -> (edge_id, region_id) cache for the
faust-regional HQ source App.

Extracted from source_app.py so it's importable for unit tests without
pulling in the protobuf binding chain (openddil.*) or faust.

Background: ADR-0028 centralizes asset -> edge -> region assignment in
asset-registry-service. cm-service and logistics-fusion emit events
with empty provenance.region_id (they have no local source-of-truth to
ask). The HQ source App's positive-match filter then dropped 100% of
those events. Phase 2 fix: subscribe to asset-registry-events, build
this cache, look up empty region_id from cache before the filter runs.

Bootstrap latency: at first-ever pod start the cache is empty; Faust
reads the compacted asset-registry-events topic from earliest so the
cache populates within seconds. Events that arrive before the cache
has the asset hit a short hold-and-retry budget
(RETRY_DELAY_S * RETRY_MAX_ATTEMPTS wall time) and are dropped with
a warning if still missing.

Restart caveat: Faust commits the cache agent's offset. After restart,
assets with no recent observations may briefly be absent from the
cache; they reappear on the next observation. A startup
SELECT-from-postgres warm path is a Phase 4 follow-up.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional


log = logging.getLogger("faust_regional.asset_registry_cache")


RETRY_DELAY_S = 0.2
RETRY_MAX_ATTEMPTS = 3


class AssetRegistryCache:
    """In-memory asset_id -> (edge_id, region_id) cache populated by
    the asset-registry-events agent on the HQ source App."""

    def __init__(self) -> None:
        self._cache: dict[str, tuple[str, str]] = {}

    def get(self, asset_id: str) -> Optional[tuple[str, str]]:
        return self._cache.get(asset_id)

    def set(self, asset_id: str, edge_id: str, region_id: str) -> None:
        self._cache[asset_id] = (edge_id, region_id)

    def size(self) -> int:
        return len(self._cache)


async def resolve_region_from_cache(
    cache: AssetRegistryCache,
    asset_id: str,
    label: str,
    *,
    retry_delay_s: float = RETRY_DELAY_S,
    retry_max_attempts: int = RETRY_MAX_ATTEMPTS,
) -> Optional[str]:
    """Look up asset_id in cache; hold-and-retry briefly to absorb
    bootstrap latency. Returns the resolved region_id, or None if
    still missing after all attempts."""
    for attempt in range(retry_max_attempts):
        hit = cache.get(asset_id)
        if hit is not None:
            return hit[1]  # region_id
        if attempt < retry_max_attempts - 1:
            await asyncio.sleep(retry_delay_s)
    log.warning(
        "%s: asset_id=%r missing from asset-registry cache after %d retries "
        "(cache size=%d) -- dropping event",
        label, asset_id, retry_max_attempts, cache.size(),
    )
    return None
