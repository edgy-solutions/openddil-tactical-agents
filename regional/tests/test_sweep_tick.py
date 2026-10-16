"""The rollup sweep runs on a tick topic because Faust refuses Table writes
outside stream iteration. Offline: the REAL aggregator app (built without a
broker) and its REAL `emitted` Table; only `send_changelog` and the output
topics' `send` are stubbed, so Faust's stream-context check stays live.

Run with: pytest regional/tests/test_sweep_tick.py -v
"""
from __future__ import annotations

import asyncio
import inspect
import types

import pytest

import aggregator_app as agg

REGION = "region-east"
TIMEOUT_S = 10


class _Rig:
    def __init__(self, monkeypatch):
        self.app = agg.make_aggregator_app(
            region_id=REGION, hq_brokers="localhost:1",
            fan_in_topic="fan-in", web_port=6066)
        self.assets = self.app.tables["region_region_east_assets_latest"]
        self.emitted = self.app.tables["region_region_east_emitted_partials"]
        self.changelog = []

        def send_changelog(partition, key, value, key_serializer=None,
                           value_serializer=None):
            self.changelog.append((key, value))
            return types.SimpleNamespace(
                message=types.SimpleNamespace(partition=partition))

        monkeypatch.setattr(self.emitted, "send_changelog", send_changelog)

        # Timers register as app tasks wrapped twice; unwrap to the body.
        self.heartbeat = inspect.unwrap(self.app._app_tasks[0])
        self.agent = self.app.agents.get("aggregator_app.on_sweep_tick")
        # The closure that holds the output topics: the tick agent, or (before
        # the sweep moved onto a tick) the heartbeat itself.
        self.sweep_body = self.agent.fun if self.agent else self.heartbeat
        self.topics = inspect.getclosurevars(self.sweep_body).nonlocals
        self.sent = []
        for name in ("out_fleet_summary", "out_top_factors", "out_wear_trends"):
            monkeypatch.setattr(self.topics[name], "send", self._recorder(name))

    def _recorder(self, name):
        async def send(key=None, value=None, **_):
            self.sent.append((name, key))
        return send

    def run(self, coro):
        # Same loop the app (and test_context) uses; asyncio.run would not be.
        return self.app.loop.run_until_complete(
            asyncio.wait_for(coro, TIMEOUT_S))

    def set_assets(self, classes):
        # Bypass the Table's stream-context check: this is test setup, not
        # code under test. {asset_id: releasable_to}
        for k in list(self.assets.data.keys()):
            del self.assets.data[k]
        for asset_id, rel in classes.items():
            self.assets.data[asset_id] = agg.AssetState(releasable_to=list(rel))


def _yield_per_tick(agent):
    """test_context needs an agent that yields (it attaches a sink). The
    production agent does not, so wrap it: feed it one tick at a time, inside
    the outer stream iteration, and yield after each. Production code is
    untouched."""
    original = agent.fun

    class _One:
        def __init__(self, item):
            self.item, self.done = item, False

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.done:
                raise StopAsyncIteration
            self.done = True
            return self.item

    async def yielding(stream):
        async for item in stream:
            await original(_One(item))
            yield item

    agent.fun = yielding


@pytest.fixture
def rig(monkeypatch):
    return _Rig(monkeypatch)


def test_heartbeat_outside_stream_only_sends_a_tick(rig, monkeypatch):
    ticks = []

    async def send(value=None, **_):
        ticks.append(value)

    tick_topic = inspect.getclosurevars(rig.heartbeat).nonlocals["sweep_tick"]
    monkeypatch.setattr(tick_topic, "send", send)
    rig.set_assets({"a1": [], "a2": ["ATL"]})
    rig.run(rig.heartbeat())
    assert ticks == [b"\x01"]
    assert dict(rig.emitted.data) == {}
    assert rig.changelog == []


def test_tick_agent_writes_emitted_inside_stream(rig):
    async def scenario():
        _yield_per_tick(rig.agent)
        async with rig.agent.test_context() as agent:
            rig.set_assets({"a1": [], "a2": ["ATL"]})
            await agent.put(b"\x01")
            first = set(rig.emitted.data.keys())
            sent_first = list(rig.sent)
            rig.sent.clear()
            rig.set_assets({"a1": []})
            await agent.put(b"\x01")
            return first, sent_first, set(rig.emitted.data.keys()), list(rig.sent)

    first, sent_first, second, sent_second = rig.run(scenario())
    assert first == {"fleet|", "fleet|ATL"}
    assert {k for _, k in sent_first} == {REGION + "|", REGION + "|ATL"}
    assert second == {"fleet|"}
    # The vanished class got its retirement row (key carries the class).
    assert (  "out_fleet_summary", REGION + "|ATL") in sent_second


def test_emit_rollups_outside_stream_is_refused_by_faust(rig):
    # Control: the harness enforces the rule the dict in
    # test_retire_partials.py cannot.
    rig.set_assets({"a1": []})
    topics = rig.topics
    with pytest.raises(TypeError, match="outside of stream iteration"):
        rig.run(agg._emit_rollups(
            region_id=REGION, assets_latest=rig.assets, emitted=rig.emitted,
            out_fleet_summary=topics["out_fleet_summary"],
            out_top_factors=topics["out_top_factors"],
            out_wear_trends=topics["out_wear_trends"]))
