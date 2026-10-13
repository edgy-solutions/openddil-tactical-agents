"""
Posture transition event: one append-only record per posture change.

Why this is on `tactical-events`: the edge decides posture transitions, but
the current posture travels on the compacted latest-state topic, which keeps
only the newest value per asset. A region behind a cut therefore never sees
the intermediate states. `tactical-events` is append-only, is forwarded at
every hop, and is projected append-only, so the transition rides there as
its own record and survives the cut.

Why `time` (and `data.at`) is the sample time: the transition happened when
the edge observed it, not when the record was built or forwarded. Wall clock
would make a replay of the same transition look like a new, later one.

Why the id is deterministic: it is a uuid5 over (asset, new posture, sample
time in ns), so a replayed or re-forwarded record carries the same id and
the projector's ON CONFLICT insert is a no-op.

Labels (originator nation, releasability) are carried from the source record
(ADR-0029 section 3). Absent stays absent: nothing is defaulted or invented,
and releasable_to is only meaningful alongside an originator nation.

This is not an alert: there is deliberately no severity.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Optional, Sequence

from posture import PostureState

EVENT_TYPE = "openddil.posture.transition"


def _rfc3339(t: float) -> str:
    return datetime.fromtimestamp(t, tz=timezone.utc).isoformat(timespec="microseconds")


def build_transition_event(
    asset_id: str,
    prev: PostureState,
    new: PostureState,
    deploy_signal: Optional[bool],
    speed: Optional[float],
    originator_nation: str,
    releasable_to: Sequence[str],
    edge_id: str,
    region_id: str,
) -> dict:
    if new.posture == prev.posture:
        raise ValueError("posture unchanged: no transition to emit")
    if new.posture_since is None:
        raise ValueError("new posture has no posture_since")

    at_ns = int(round(new.posture_since * 1e9))
    at = _rfc3339(new.posture_since)
    data = {
        "from": prev.posture,
        "to": new.posture,
        "at": at,
        "motion_since": _rfc3339(new.motion_since) if new.motion_since is not None else None,
        "motion": new.motion,
        "deploy_signal": deploy_signal,
        "speed_mps": speed,
        "edge_id": edge_id,
        "region_id": region_id,
    }
    if originator_nation:
        data["originator_nation"] = originator_nation
        data["releasable_to"] = list(releasable_to)
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid5(
            uuid.NAMESPACE_URL, f"openddil:posture:{asset_id}:{new.posture}:{at_ns}"
        )),
        "source": f"openddil/edge/{edge_id}/{asset_id}",
        "type": EVENT_TYPE,
        "subject": asset_id,
        "time": at,
        "datacontenttype": "application/json",
        "data": data,
    }
