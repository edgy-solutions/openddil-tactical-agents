"""The pure per-sample core: one raw message in, new state and sends out.

No faust import, so everything the runner does per record is testable
without a broker. The order is the engine's original one: the state is
written first, then outputs are built and sent, so a failure while building
an output never loses the accumulated state.
"""
from __future__ import annotations

import logging
import os
from collections import Counter

from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import outputs, roles
from algorithm_runtime.declaration import Declaration, config_hash

log = logging.getLogger("algorithm_runtime")


def default_state(decl: Declaration) -> dict:
    return {f.name: f.default for f in decl.state_fields}


def _stamp_hash(unit, decl: Declaration) -> str:
    h = getattr(unit, "_config_hash", None)
    if h is None:
        h = config_hash(decl.unit, decl.version, unit.params)
        unit._config_hash = h
    return h


def process_one(unit, decl: Declaration, raw: bytes, state: dict | None):
    """Returns (new_state or None, [(topic, key_bytes, message)], counters).

    A None state means "write nothing": the sample was skipped (unparseable,
    no asset_id, a required role absent) or `step` raised. Skips never touch
    state."""
    counters: Counter = Counter()
    try:
        evt = pb.EntityTelemetryEvent()
        evt.ParseFromString(raw)
    except Exception as exc:  # noqa: BLE001 - parse errors are skip-not-crash
        log.warning("%s: failed to parse EntityTelemetryEvent: %s", decl.unit, exc)
        counters["parse_failed"] += 1
        return None, [], counters

    asset_id = evt.asset.asset_id
    if not asset_id:
        counters["no_asset_id"] += 1
        return None, [], counters

    extracted = roles.extract(evt, {r.role for r in decl.roles})
    if extracted.pop("sample_time_fallback", False):
        counters["sample_time_fallback"] += 1
    for _role in extracted.pop("role_unconvertible", ()):
        counters["role_unconvertible"] += 1
    sample = {r.role: extracted[r.role] for r in decl.roles if r.role in extracted}
    missing = [r.role for r in decl.roles if r.required and r.role not in sample]
    if missing:
        counters["required_role_absent"] += 1
        return None, [], counters

    work = dict(state) if state is not None else default_state(decl)
    try:
        result = unit.step(sample, work)
    except Exception:  # noqa: BLE001 - a unit bug skips the sample, never the stream
        log.exception("%s: step raised for asset %s; sample skipped", decl.unit, asset_id)
        counters["step_failed"] += 1
        return None, [], counters

    if set(work) != {f.name for f in decl.state_fields}:
        # State is the declared fields and nothing else; the table record
        # could not hold more, and a silently dropped field is a lost one.
        log.error("%s: step changed the state's keys for asset %s; sample skipped", decl.unit, asset_id)
        counters["step_failed"] += 1
        return None, [], counters

    sends = []
    stamp = _stamp_hash(unit, decl)
    tier = os.environ["OPENDDIL_EDGE_ID"]
    declared = {o.kind: o for o in decl.outputs}
    for kind, payload in (result or {}).items():
        out = declared.get(kind)
        try:
            if out is None:
                raise outputs.OutputError(f"{decl.unit} returned undeclared output kind {kind!r}")
            msg = outputs.build_payload(kind, payload)
            evt_out = outputs.build_event(
                evt, msg, out, unit=decl.unit, version=decl.version,
                config_hash=stamp, tier=tier,
            )
        except outputs.OutputError as exc:
            log.warning("%s: output refused for asset %s: %s", decl.unit, asset_id, exc)
            counters["output_refused"] += 1
            continue
        sends.append((out.topic, asset_id.encode(), evt_out))
    return work, sends, counters
