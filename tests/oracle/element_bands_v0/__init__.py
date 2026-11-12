"""The old in-line element band path as an oracle for the replay harness's
--compare-oracle mode: element_bands.py and element_band_event.py as they were
in edge/, `band_events_for_envelope` as it was in faust_edge.py, and the gates
and band block of the old `process_elements` agent, with the broker taken out.

The one thing not frozen here is `rollup_from_envelope`: it is imported live
from edge/element_rollup.py, because the element rollup is not being ported on
this branch and is still the code `process_elements` runs.

The old path had no handler around the rollup: an envelope that makes it raise
(a non-dict element, `elements` not a list, an infinite `observed_at_ns`) raised
out of the agent. The oracle keeps that, so `feed` raises on those envelopes;
the unit skips them and counts `rollup_raised`.

`Oracle(params, edge_id, region_id).feed(raw, record_time_ns)` returns
[(kind, message)] as the harness wants; `sends(raw)` returns what the old agent
sent to tactical-events, as (topic, key, value bytes) in order.
"""
from __future__ import annotations

import json
from collections.abc import Mapping

from element_rollup import rollup_from_envelope
from element_bands_v0.element_band_event import build_band_event
from element_bands_v0.element_bands import BandTracker


def band_events_for_envelope(env: dict, tracker, config: dict,
                             edge_id: str, region_id: str) -> list[dict]:
    """Band-crossing events for one decoded element envelope. Sample time is
    the envelope's observed_at_ns (the instant the rollup carries); an
    envelope without one yields nothing rather than a guessed time. Labels
    come from the envelope, as for the element window."""
    aid = env.get("asset_id")
    obs_ns = env.get("observed_at_ns")
    elements = env.get("elements")
    if (not aid or not isinstance(aid, str) or not elements
            or isinstance(obs_ns, bool) or not isinstance(obs_ns, (int, float))):
        return []
    nation = env.get("originator_nation") or ""
    releasable = env.get("releasable_to") or []
    if not isinstance(releasable, list):
        releasable = []
    releasable = [r for r in releasable if isinstance(r, str)]
    out = []
    for el in elements:
        if not isinstance(el, dict):
            continue
        path = el.get("element_id")
        if not path or not isinstance(path, str):
            continue
        for field, spec in config.items():
            value = el.get(field)
            crossing = tracker.update(aid, path, field, value)
            if crossing is None:
                continue
            frm, to = crossing
            out.append(build_band_event(
                aid, path, el.get("layer_name") or "", field, frm, to,
                float(value), obs_ns / 1e9, spec["severity"][to],
                nation if isinstance(nation, str) else "", releasable,
                edge_id, region_id,
            ))
    return out


class Oracle:
    kinds = ("band_crossing",)

    def __init__(self, params: Mapping, edge_id: str, region_id: str):
        self.config = params["bands"]
        self.tracker = BandTracker(self.config)
        self.edge_id, self.region_id = edge_id, region_id

    def sends(self, raw: bytes) -> list:
        """The old agent's body for one element envelope, from the gates on:
        [(topic, key, value bytes)]."""
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
        return [("tactical-events", aid, json.dumps(ev).encode("utf-8"))
                for ev in band_events_for_envelope(
                    env, self.tracker, self.config, self.edge_id, self.region_id)]

    def feed(self, raw: bytes, record_time_ns: int) -> list:
        """[(kind, message)]: each crossing as the dict it was before json.dumps."""
        return [("band_crossing", json.loads(value)) for _t, _k, value in self.sends(raw)]
