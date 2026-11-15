"""The old in-line element window path as an oracle for the replay harness's
--compare-oracle mode: `rollup_from_envelope` as it was in edge/element_rollup.py
(rollup.py here), `_window_from_envelope` as it was in faust_edge.py, and the
gates of the old `process_elements` agent, with the broker taken out.

Two things are taken live and not frozen: `detection.windows.build_window_spec`
(the window span) and `algorithm_runtime.outputs.build_payload("window", ...)`
(the trends payload as a message), which the old path also called. They are the
`trends` unit's, which another test freezes.

The old path had no handler around the rollup: an envelope that makes it raise
(a non-dict element, `elements` not a list, an infinite `observed_at_ns`) raised
out of the agent. The oracle keeps that, so `feed` raises on those envelopes;
the unit skips them and counts `rollup_raised`.

`Oracle(params, edge_id, region_id)` (params unused: the old path had its
thresholds as module constants) takes "now" through `set_clock(fn)` (ns; the
wall clock until then) and the trends window through `last_window` (a callable
asset_id -> the unit's last `window` payload or None; none until set).
`feed(raw, record_time_ns)` returns [(kind, message)] as the harness wants;
`sends(raw)` returns what the old agent sent to the windows topic, as
(topic, key, value bytes).
"""
from __future__ import annotations

import json
import logging
import time
from collections.abc import Mapping

from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb

from element_rollup_v0.rollup import rollup_from_envelope

logger = logging.getLogger(__name__)


def _window_from_envelope(env: dict, rollup: winpb.ElementRollup, now_ns: int,
                          last_window, edge_id: str, region_id: str) -> winpb.WindowedTelemetry:
    """Window for the element path: the window spans the snapshot's own
    observation instant, and labels come from the envelope (ADR-0029
    PROPAGATE, no default: an envelope without labels gives an unlabelled
    window, which the egress gate then refuses). The sustainment trends are
    the `trends` unit's last window for this asset, when it has emitted one."""
    from algorithm_runtime import outputs as algorithm_outputs
    from detection.windows import build_window_spec
    aid = env.get("asset_id") or ""
    obs_ns = (rollup.observed_at.ToNanoseconds()
              if rollup.HasField("observed_at") else now_ns)
    releasable = env.get("releasable_to") or []
    if not isinstance(releasable, list):
        releasable = []
    nation = env.get("originator_nation") or ""
    last = last_window(aid) if last_window is not None else None

    out = winpb.WindowedTelemetry()
    trend_samples = 0
    if last is not None:
        try:
            out.CopyFrom(algorithm_outputs.build_payload("window", last))
            trend_samples = int(last["sample_count"])
        except (algorithm_outputs.OutputError, KeyError) as exc:
            logger.warning("element window for %s: last trends payload unusable: %s", aid, exc)
            out = winpb.WindowedTelemetry()
    out.asset_id = aid
    out.platform_variant = env.get("platform_variant") or ""
    out.computed_at.FromNanoseconds(now_ns)
    out.provenance.producer_id = "faust-edge"
    out.provenance.edge_id = edge_id
    out.provenance.region_id = region_id
    out.provenance.ingest_time.FromNanoseconds(now_ns)
    out.provenance.classification = "U"
    if isinstance(nation, str) and nation:
        out.provenance.originator_nation = nation
    out.provenance.releasable_to.extend(r for r in releasable if isinstance(r, str))
    out.window.CopyFrom(build_window_spec(
        window_start_ns=obs_ns,
        window_end_ns=obs_ns,
        sample_count=trend_samples,
    ))
    out.element_rollup.CopyFrom(rollup)
    return out


class Oracle:
    kinds = ("element_window",)

    def __init__(self, params: Mapping, edge_id: str, region_id: str, last_window=None):
        self.edge_id = edge_id
        self.region_id = region_id or "region-01"   # the old module's env default
        self.last_window = last_window
        self.now = time.time_ns
        self.rollups: dict[str, winpb.ElementRollup] = {}   # the old `element_rollups`

    def set_clock(self, fn) -> None:
        self.now = fn or time.time_ns

    def sends(self, raw: bytes) -> list:
        """The old agent's body for one element envelope, from the parse on:
        [(topic, key, value bytes)]. Raises where the old agent raised."""
        try:
            env = json.loads(raw)
            if not isinstance(env, dict):
                raise ValueError("envelope is not an object")
        except Exception:  # noqa: BLE001 - the old path logged and moved on
            return []
        aid = env.get("asset_id")
        if not aid or not isinstance(aid, str):
            return []
        rollup = rollup_from_envelope(env)
        if rollup is None:
            return []
        self.rollups[aid] = rollup
        w = _window_from_envelope(env, rollup, self.now(), self.last_window,
                                  self.edge_id, self.region_id)
        return [("asset-telemetry-windows", aid.encode(), w.SerializeToString())]

    def feed(self, raw: bytes, record_time_ns: int) -> list:
        """[(kind, message)]: the window as the message the old agent serialized."""
        out = []
        for _t, _k, value in self.sends(raw):
            w = winpb.WindowedTelemetry()
            w.ParseFromString(value)
            out.append(("element_window", w))
        return out
