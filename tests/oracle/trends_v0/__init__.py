"""The old rolling-window path as an oracle for the replay harness's
--compare-oracle mode. See `Oracle` for the interface a `<name>_v0` package
offers the harness."""
from __future__ import annotations

from collections.abc import Mapping

from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from openddil.telemetry.v1 import telemetry_pb2 as pb

from trends_v0.frozen import OldTrends


class Oracle:
    """What `--compare-oracle <name>` imports from `<name>_v0`.

    `Oracle(params, edge_id, region_id)` takes the unit's resolved params (so
    both sides run on one configuration) and the tier's ids.
    `feed(raw, record_time_ns)` runs the old path on one raw record, with the
    record's time as its clock, and returns the outputs it would have sent as
    [(kind, message)], in order. The module may also define `normalize(kind,
    msg)`, applied to both sides before the comparison, and `synth`, a module
    with `make(seed)` returning a function that fills a decoded event in place
    (see `--synth-seed`)."""

    kinds = ("window",)

    def __init__(self, params: Mapping, edge_id: str, region_id: str):
        self.old = OldTrends(
            fluid_window_ns=int(float(params["fluid_window_min"]) * 60 * 1e9),
            wear_window_ns=int(float(params["wear_window_min"]) * 60 * 1e9),
            emit_every_n=int(params["emit_every_n_samples"]),
            deque_cap=int(params["window_cap"]),
            edge_id=edge_id, region_id=region_id,
        )

    def feed(self, raw: bytes, record_time_ns: int) -> list:
        evt = pb.EntityTelemetryEvent()
        try:
            evt.ParseFromString(raw)
        except Exception:  # noqa: BLE001 - the old path logged and moved on
            return []
        w = self.old.step_1a(evt, record_time_ns)
        return [("window", w)] if w is not None else []


def normalize(kind: str, msg):
    """The old wear_trends order followed set iteration (hash-seed dependent);
    the unit sorts by component_key. Both sides are put in that order."""
    if kind != "window":
        return msg
    out = winpb.WindowedTelemetry()
    out.CopyFrom(msg)
    items = sorted(out.wear_trends, key=lambda t: t.component_key)
    copies = []
    for t in items:
        c = winpb.ComponentWearTrend()
        c.CopyFrom(t)
        copies.append(c)
    del out.wear_trends[:]
    out.wear_trends.extend(copies)
    return out
