"""Unit tests for the asset-registry cache + resolve helper used by the
faust-regional HQ source App to plug the empty-region_id rollup gap
(ADR-0028 Phase 2).

No Kafka, no Faust, no protobuf. The cache is a plain dict; the
resolver is a small async function with a short retry budget. Both
land directly on the load-bearing path of the Phase 2 fix and are
worth pinning behavior on.

Run with: pytest regional/tests/test_asset_registry_cache.py -v
"""
from __future__ import annotations

import asyncio
import time

import pytest

from asset_registry_cache import (
    AssetRegistryCache,
    resolve_region_from_cache,
)


# ---------------------------------------------------------------------------
# Cache class (pure-dict, no async)
# ---------------------------------------------------------------------------
def test_empty_cache_misses():
    c = AssetRegistryCache()
    assert c.get("asset-001") is None
    assert c.size() == 0


def test_set_then_get_returns_pair():
    c = AssetRegistryCache()
    c.set("asset-001", "edge-01", "region-east")
    assert c.get("asset-001") == ("edge-01", "region-east")
    assert c.size() == 1


def test_set_overwrites_previous_value():
    """Re-assignment is real (memory: 'they change and the system must
    be able to help the human avoid the mistakes'). When the registry
    publishes a new mapping for an asset, the cache must reflect it.
    """
    c = AssetRegistryCache()
    c.set("asset-001", "edge-01", "region-east")
    c.set("asset-001", "edge-02", "region-west")
    assert c.get("asset-001") == ("edge-02", "region-west")
    assert c.size() == 1  # still one asset, just remapped


def test_multiple_assets_independent():
    c = AssetRegistryCache()
    c.set("asset-001", "edge-01", "region-east")
    c.set("asset-002", "edge-02", "region-west")
    assert c.get("asset-001") == ("edge-01", "region-east")
    assert c.get("asset-002") == ("edge-02", "region-west")
    assert c.size() == 2


# ---------------------------------------------------------------------------
# resolve_region_from_cache (async, with retry budget)
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_resolve_hit_on_first_try_returns_region_id():
    """Cache pre-populated -> single get() -> returns region_id, no
    sleep. The common-case fast path: registry observation lands
    before the cm-state/logistics-status event arrives."""
    c = AssetRegistryCache()
    c.set("asset-001", "edge-01", "region-east")

    t0 = time.monotonic()
    region = await resolve_region_from_cache(
        c, "asset-001", "test-label",
        retry_delay_s=10.0,   # would dominate wall-time if we ever slept
        retry_max_attempts=3,
    )
    elapsed = time.monotonic() - t0

    assert region == "region-east"
    # Should be effectively instant; assert nowhere near the
    # huge retry_delay_s we configured.
    assert elapsed < 0.1, f"unexpected sleep on cache-hit path: {elapsed}s"


@pytest.mark.asyncio
async def test_resolve_returns_only_region_not_edge():
    """Helper returns the region_id ONLY; the source App's job is to
    compare against its own configured region. Surfacing the edge_id
    would invite callers to start trusting cache-derived edges for
    routing, which is a Phase 4 concern (divergence handling)."""
    c = AssetRegistryCache()
    c.set("asset-001", "edge-NORTH", "region-east")
    region = await resolve_region_from_cache(c, "asset-001", "test")
    assert region == "region-east"
    assert region != "edge-NORTH"


@pytest.mark.asyncio
async def test_resolve_miss_returns_none_after_full_budget():
    """Asset never appears in the cache -> after RETRY_MAX_ATTEMPTS,
    return None. Source App should drop the event, NOT loop forever.

    Speed control: drive retry_delay_s to ~0 so the test doesn't
    pay the full hold-and-retry budget on every CI run. The
    behavior we're pinning is the result, not the wall-time."""
    c = AssetRegistryCache()
    region = await resolve_region_from_cache(
        c, "asset-001", "test",
        retry_delay_s=0.001, retry_max_attempts=3,
    )
    assert region is None


@pytest.mark.asyncio
async def test_resolve_recovers_when_cache_populates_mid_retry():
    """Bootstrap-race scenario: cm-state event for asset-001 arrives
    while asset-registry-events for it is still in flight. The retry
    budget MUST give the populating-agent a chance to win the race.

    Mechanism: launch the resolver, then populate the cache from a
    second coroutine after a delay shorter than retry_delay_s, then
    await the resolver and check it returned the right region."""
    c = AssetRegistryCache()

    async def populate_after(delay: float):
        await asyncio.sleep(delay)
        c.set("asset-001", "edge-01", "region-east")

    resolver = asyncio.create_task(resolve_region_from_cache(
        c, "asset-001", "test",
        retry_delay_s=0.05, retry_max_attempts=5,
    ))
    populator = asyncio.create_task(populate_after(0.06))

    region = await resolver
    await populator

    assert region == "region-east"


@pytest.mark.asyncio
async def test_resolve_zero_attempts_returns_none_immediately():
    """Edge case: retry_max_attempts=0 -> no get() call, no sleep,
    return None. Useful as a knob for tests of upstream paths that
    don't want the cache lookup interfering."""
    c = AssetRegistryCache()
    c.set("asset-001", "edge-01", "region-east")
    region = await resolve_region_from_cache(
        c, "asset-001", "test",
        retry_delay_s=10.0, retry_max_attempts=0,
    )
    assert region is None


@pytest.mark.asyncio
async def test_resolve_logs_warning_on_persistent_miss(caplog):
    """A persistent miss writes a structured warning so operators can
    see when bootstrap is taking longer than the retry budget. We
    don't pin the exact message text -- but we DO pin that:
      - log level is WARNING (not ERROR; misses during bootstrap are
        expected and shouldn't page anyone)
      - the asset_id, label, and cache size are all included so the
        operator can correlate by asset and see how warm the cache is.
    """
    import logging
    c = AssetRegistryCache()
    c.set("other-asset", "edge-99", "region-other")  # so size != 0

    with caplog.at_level(logging.WARNING, logger="faust_regional.asset_registry_cache"):
        region = await resolve_region_from_cache(
            c, "asset-MISSING", "region-east-hq-source/cm-state",
            retry_delay_s=0.001, retry_max_attempts=2,
        )

    assert region is None
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "asset-MISSING" in msg
    assert "region-east-hq-source/cm-state" in msg
    assert "cache size=1" in msg
