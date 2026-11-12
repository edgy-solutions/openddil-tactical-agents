"""The old in-line posture path as an oracle for the replay harness's
--compare-oracle mode: posture.py and posture_event.py as they were in
edge/, and the posture block and step 1 of the old `process` agent, with the
broker and the Faust Table taken out (a dict keyed by asset_id stands in for
the Table, and `signal_ever_seen` is the old module-level dict).

`Oracle(params, edge_id, region_id).feed(raw, record_time_ns)` returns
[(kind, message)] as the harness wants; `sends(raw)` returns what the old
agent sent, as (topic, key, value bytes) in order, for the exact-bytes test.
The old path ignored the broker time and used the event's own sample time.
"""
from __future__ import annotations

import json
from collections.abc import Mapping

from openddil.telemetry.v1 import telemetry_pb2 as pb

from posture_v0 import posture as posture_mod
from posture_v0.posture_event import build_transition_event

# posture.py's lower-case state names <-> the proto enum (as in the old file).
_POSTURE_TO_PROTO = {
    posture_mod.UNSPECIFIED:   pb.POSTURE_STATUS_UNSPECIFIED,
    posture_mod.EMPLACED:      pb.POSTURE_STATUS_EMPLACED,
    posture_mod.MARCH_ORDERED: pb.POSTURE_STATUS_MARCH_ORDERED,
    posture_mod.MOVING:        pb.POSTURE_STATUS_MOVING,
    posture_mod.EMPLACING:     pb.POSTURE_STATUS_EMPLACING,
}


def _posture_inputs(evt):
    """The old `_posture_inputs`, verbatim."""
    op = evt.operational_state
    if op.HasField("launcher_raised"):
        deploy_signal = op.launcher_raised
    elif op.HasField("emitting"):
        deploy_signal = op.emitting
    else:
        deploy_signal = None
    vel = evt.kinematics.velocity
    if vel.WhichOneof("frame") == "ecef":
        v = vel.ecef
        speed = (v.x * v.x + v.y * v.y + v.z * v.z) ** 0.5
    else:
        speed = None
    ts = evt.provenance.sample_time
    now = ts.seconds + ts.nanos / 1e9
    return deploy_signal, speed, now


class Oracle:
    kinds = ("latest_state", "posture_transition")

    def __init__(self, params: Mapping, edge_id: str, region_id: str,
                 initial: Mapping | None = None):
        self.th = posture_mod.Thresholds(
            move_speed_mps=float(params["move_speed_mps"]),
            move_hold_s=float(params["move_hold_s"]),
            stop_hold_s=float(params["stop_hold_s"]),
        )
        self.edge_id, self.region_id = edge_id, region_id
        # asset_id -> the StateRecord's posture fields, as a dict.
        self.table: dict[str, dict] = {k: dict(v) for k, v in (initial or {}).items()}
        self.signal_ever_seen: dict[str, bool] = {}

    def _rec(self, aid: str) -> dict:
        return self.table.setdefault(aid, {
            "posture": posture_mod.UNSPECIFIED, "posture_since": None,
            "motion": "", "motion_since": None})

    def sends(self, raw: bytes) -> list:
        """The old agent's body for one record: [(topic, key, value bytes)]."""
        evt = pb.EntityTelemetryEvent()
        try:
            evt.ParseFromString(raw)
        except Exception:  # noqa: BLE001 - the old path logged and moved on
            return []
        out = []
        aid = evt.asset.asset_id
        rec = self._rec(aid)
        deploy_signal, speed, posture_now = _posture_inputs(evt)
        if deploy_signal is not None:
            self.signal_ever_seen[aid] = True
        prev = posture_mod.PostureState(
            posture=rec["posture"], posture_since=rec["posture_since"],
            motion=rec["motion"], motion_since=rec["motion_since"])
        new = posture_mod.step(prev, deploy_signal, speed, posture_now, self.th)
        rec["posture"], rec["posture_since"] = new.posture, new.posture_since
        rec["motion"], rec["motion_since"] = new.motion, new.motion_since

        untouched = new.posture == posture_mod.UNSPECIFIED and not self.signal_ever_seen.get(aid, False)
        if untouched:
            out.append(("telemetry-latest-state", aid, raw))
        else:
            if new.posture == posture_mod.UNSPECIFIED:
                evt.operational_state.ClearField("posture_since")
            else:
                evt.operational_state.posture_since.FromNanoseconds(int(round(new.posture_since * 1e9)))
            evt.operational_state.posture_status = _POSTURE_TO_PROTO[new.posture]
            out.append(("telemetry-latest-state", aid, evt.SerializeToString()))
        if new.posture != prev.posture:
            out.append(("tactical-events", aid, json.dumps(build_transition_event(
                aid, prev, new, deploy_signal, speed,
                evt.provenance.originator_nation, list(evt.provenance.releasable_to),
                self.edge_id, self.region_id)).encode("utf-8")))
        return out

    def feed(self, raw: bytes, record_time_ns: int) -> list:
        """[(kind, message)]: the latest-state bytes, and the transition as the
        dict it was before json.dumps."""
        res = []
        for topic, _key, value in self.sends(raw):
            if topic == "telemetry-latest-state":
                res.append(("latest_state", value))
            else:
                res.append(("posture_transition", json.loads(value)))
        return res
