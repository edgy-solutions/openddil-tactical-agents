"""
Element band event: one append-only record per element health-band crossing.

Why this is on `tactical-events`: element state travels on a compacted
latest-state path, which keeps only the newest value per key. A region behind
a cut therefore never sees that an element crossed a band and came back.
`tactical-events` is append-only and forwarded at every hop, so the crossing
rides there as its own record and survives the cut.

Why `time` (and `data.at`) is the sample time: the crossing happened when the
edge observed the element, not when the record was built or forwarded. Wall
clock would make a replay of the same crossing look like a new, later one.

Why the id is deterministic: it is a uuid5 over (asset, element, field, new
band, sample time in ns), so a replayed or re-forwarded record carries the
same id and the projector's ON CONFLICT insert is a no-op.

Severity is passed in by the caller, taken from the band configuration. This
module never defaults it: a band without a configured severity is a config
error caught when the config is parsed, not something to paper over here.

Labels (originator nation, releasability) are carried from the source record
(ADR-0029 section 3). Absent stays absent: nothing is defaulted or invented,
and releasable_to is only meaningful alongside an originator nation.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Sequence

EVENT_TYPE = "openddil.element.band"


def _rfc3339(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).isoformat(timespec="microseconds")


def build_band_event(
    asset_id: str,
    element_path: str,
    layer_name: str,
    field: str,
    from_band: str,
    to_band: str,
    value: float,
    at: float,
    severity: str,
    originator_nation: str,
    releasable_to: Sequence[str],
    edge_id: str,
    region_id: str,
) -> dict:
    if from_band == to_band:
        raise ValueError("band unchanged: no crossing to emit")

    at_ns = int(round(at * 1e9))
    at_s = _rfc3339(at)
    data = {
        "asset_id": asset_id,
        "element_path": element_path,
        "layer_name": layer_name,
        "field": field,
        "from": from_band,
        "to": to_band,
        "value": value,
        "at": at_s,
        "severity": severity,
        "edge_id": edge_id,
        "region_id": region_id,
    }
    if originator_nation:
        data["originator_nation"] = originator_nation
        data["releasable_to"] = list(releasable_to)
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"openddil:element-band:{asset_id}:{element_path}:{field}:{to_band}:{at_ns}",
        )),
        "source": f"openddil/edge/{edge_id}/{asset_id}",
        "type": EVENT_TYPE,
        "subject": asset_id,
        "time": at_s,
        "datacontenttype": "application/json",
        "data": data,
    }
