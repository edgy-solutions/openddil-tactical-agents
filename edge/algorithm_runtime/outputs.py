"""Output kinds -> proto envelopes.

A unit returns a payload dict per output kind; this module turns it into the
message on the wire. The source class of every value a unit emits (DERIVED,
confidence 0.0) is the contract's to state, never the unit's, so a payload
that tries to carry its own `value_provenance` is refused. The same holds for
an anomaly payload: severity, labels, provenance and the rest of the envelope
are stated here from the declaration and the source, never by the unit.

The `window` kind: a WindowedTelemetry on the windows topic. The unit's
payload is plain numbers, all required unless marked optional:

    {"window_start_ns": int, "window_end_ns": int, "sample_count": int,
     "fluid_trends": {signal: TREND},
     "consumable_trends": [{"slot_key": str, "remaining": TREND,
                            "capacity": number (optional), "nsn": str (optional)}],
     "wear_trends": [{"component_key": str, "hours_in_service": TREND (optional),
                      "remaining_useful_life": TREND (optional)}],
     "active_fault_codes": [str]}
    TREND = {"latest": float, "latest_unit": str, "slope": float, "slope_unit": str,
             "r_squared": float, "latest_sample_ns": int, "window_start_ns": int,
             "sample_count": int}

A unit is a pint unit name, as `str(pint units)`; the slope is per hour and is
given in `slope_unit`, which the builder turns into `slope_unit + "/h"`
through pint, so the strings on the wire are formatted as the original window
path formatted them (`detection.windows`). The envelope (asset, labels,
provenance, computed_at, the element rollup) is the contract's, as above.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Mapping

from google.protobuf import json_format
from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime.declaration import OutputDecl
from element_band_event import build_band_event
from posture_event import build_transition_event


class OutputError(ValueError):
    pass


# The posture unit's lower-case state names <-> the proto enum. Lower-case
# without prefix is also the projector's on-disk representation
# (telemetry_latest.py), so this mapping and that one must be kept in agreement.
_POSTURE_TO_PROTO = {
    "unspecified":   pb.POSTURE_STATUS_UNSPECIFIED,
    "emplaced":      pb.POSTURE_STATUS_EMPLACED,
    "march_ordered": pb.POSTURE_STATUS_MARCH_ORDERED,
    "moving":        pb.POSTURE_STATUS_MOVING,
    "emplacing":     pb.POSTURE_STATUS_EMPLACING,
}
_POSTURE_FIELDS = ("posture", "posture_since", "motion", "motion_since")


def _latest_state_payload(payload: Mapping) -> dict:
    """`{"untouched": True}` (send the input bytes as they came) or
    `{"posture": name, "posture_since": float | None}`."""
    if isinstance(payload, Mapping) and dict(payload) == {"untouched": True}:
        return {"untouched": True}
    if not isinstance(payload, Mapping) or set(payload) != {"posture", "posture_since"}:
        raise OutputError(
            'latest_state payload must be {"untouched": True} or {"posture": ..., "posture_since": ...}')
    posture, since = payload["posture"], payload["posture_since"]
    if posture not in _POSTURE_TO_PROTO:
        raise OutputError(f"latest_state posture {posture!r} is not one of {sorted(_POSTURE_TO_PROTO)}")
    if posture != "unspecified" and not _is_number(since):
        raise OutputError("latest_state posture_since must be a number unless the posture is unspecified")
    return {"posture": posture, "posture_since": since}


def _posture_dict(v, where: str) -> dict:
    if not isinstance(v, Mapping) or set(v) != set(_POSTURE_FIELDS):
        raise OutputError(f"posture_transition {where} must hold exactly {list(_POSTURE_FIELDS)}")
    return dict(v)


def _transition_payload(payload: Mapping) -> dict:
    if not isinstance(payload, Mapping) or set(payload) != {"prev", "new", "deploy_signal", "speed"}:
        raise OutputError("posture_transition payload must carry exactly prev, new, deploy_signal and speed")
    return {"prev": _posture_dict(payload["prev"], "prev"), "new": _posture_dict(payload["new"], "new"),
            "deploy_signal": payload["deploy_signal"], "speed": payload["speed"]}


_BAND_KEYS = {"element_id", "layer_name", "field", "from", "to", "value"}


def _band_payload(payload: Mapping) -> dict:
    """One crossing, as the unit states it. Severity, time and labels are the
    host's, so a payload carrying any of them is refused."""
    if not isinstance(payload, Mapping) or set(payload) != _BAND_KEYS:
        raise OutputError(
            "band_crossing payload must carry exactly element_id, layer_name, field, from, to and value "
            "(severity, time and labels are the host's)")
    if not all(isinstance(payload[k], str) for k in ("element_id", "field", "from", "to")):
        raise OutputError("band_crossing element_id, field, from and to must be strings")
    if not _is_number(payload["value"]):
        raise OutputError("band_crossing value must be a number")
    return dict(payload)


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _anomaly_payload(payload: Mapping) -> dict:
    """The only two things a unit says about an anomaly: what happened, in
    words, and the numbers behind it."""
    extra = sorted(set(payload) - {"summary", "evidence"})
    if extra:
        raise OutputError(
            f"anomaly payload carries {extra}; only summary and evidence are the unit's to state "
            "(severity, labels, provenance and the envelope are the contract's)")
    if set(payload) != {"summary", "evidence"}:
        raise OutputError("anomaly payload must carry exactly summary and evidence")
    if not isinstance(payload["summary"], str):
        raise OutputError("anomaly payload summary must be a string")
    evidence = payload["evidence"]
    if not isinstance(evidence, Mapping) or not all(
            isinstance(k, str) and _is_number(v) for k, v in evidence.items()):
        raise OutputError("anomaly payload evidence must map strings to numbers")
    return {"summary": payload["summary"], "evidence": dict(evidence)}


def _trend_proto(t: Mapping) -> winpb.ScalarTrend:
    """A TREND dict as the wire ScalarTrend, through `detection.windows` so
    pint formats the unit strings."""
    from detection import windows  # pint is slow to import; only a window output pays for it
    try:
        latest = windows._ureg.Quantity(float(t["latest"]), t["latest_unit"])
        slope = windows._ureg.Quantity(float(t["slope"]), t["slope_unit"] + "/h")
        return windows.trend_to_proto(windows.TrendResult(
            latest=latest, slope=slope, r_squared=float(t["r_squared"]),
            sample_count=int(t["sample_count"]), window_start_ns=int(t["window_start_ns"]),
            window_end_ns=0, latest_sample_ns=int(t["latest_sample_ns"])))
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise OutputError(f"window trend is malformed: {exc!r}") from exc
    except Exception as exc:  # noqa: BLE001 - pint's undefined-unit errors
        raise OutputError(f"window trend has a unit pint cannot read: {exc}") from exc


def _window_payload(payload: Mapping) -> winpb.WindowedTelemetry:
    from detection import windows
    try:
        out = winpb.WindowedTelemetry()
        for signal, t in payload["fluid_trends"].items():
            out.fluid_trends[signal].CopyFrom(_trend_proto(t))
        for c in payload["consumable_trends"]:
            item = out.consumable_trends.add()
            item.slot_key = c["slot_key"]
            item.remaining.CopyFrom(_trend_proto(c["remaining"]))
            if "capacity" in c:
                item.capacity = c["capacity"]
            if "nsn" in c:
                item.nsn = c["nsn"]
        for w in payload["wear_trends"]:
            item = out.wear_trends.add()
            item.component_key = w["component_key"]
            if "hours_in_service" in w:
                item.hours_in_service.CopyFrom(_trend_proto(w["hours_in_service"]))
            if "remaining_useful_life" in w:
                item.remaining_useful_life.CopyFrom(_trend_proto(w["remaining_useful_life"]))
        out.active_fault_codes_latest.extend(payload["active_fault_codes"])
        out.window.CopyFrom(windows.build_window_spec(
            window_start_ns=int(payload["window_start_ns"]),
            window_end_ns=int(payload["window_end_ns"]),
            sample_count=int(payload["sample_count"])))
    except OutputError:
        raise
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise OutputError(f"payload is not a window: {exc!r}") from exc
    return out


def build_payload(kind: str, payload: Mapping) -> pb.SustainmentMetrics | winpb.WindowedTelemetry | dict:
    """Typed message for one output kind (a dict for an anomaly: its wire
    form is JSON, not a proto)."""
    if kind == "anomaly":
        return _anomaly_payload(payload)
    if kind == "window":
        return _window_payload(payload)
    if kind == "latest_state":
        return _latest_state_payload(payload)
    if kind == "posture_transition":
        return _transition_payload(payload)
    if kind == "band_crossing":
        return _band_payload(payload)
    if kind != "sustainment":
        raise OutputError(f"unknown output kind {kind!r}")
    if "value_provenance" in payload:
        raise OutputError(
            "payload carries value_provenance; the source class is the contract's, not the unit's")
    try:
        msg = json_format.ParseDict(dict(payload), pb.SustainmentMetrics())
    except json_format.ParseError as exc:
        raise OutputError(f"payload is not a SustainmentMetrics: {exc}") from exc
    # 0.0 is the ACCURATE value: there is no oracle, so there is nothing to be
    # 0.95 confident in. Real confidence arrives with the validation phase
    # (ADR-0020). A placeholder masquerading as a real number is the failure
    # mode this framing exists to prevent.
    msg.value_provenance["*"].origin = pb.ORIGIN_DERIVED
    msg.value_provenance["*"].confidence = 0.0
    return msg


def encode(msg) -> bytes:
    """Wire bytes of a built output: a proto serialises, an anomaly's
    CloudEvent dict is JSON."""
    if isinstance(msg, dict):
        return json.dumps(msg).encode("utf-8")
    if isinstance(msg, bytes):
        return msg
    return msg.SerializeToString()


def build_event(
    src: pb.EntityTelemetryEvent,
    sustainment: pb.SustainmentMetrics | dict,
    out: OutputDecl,
    *,
    unit: str,
    version: str,
    config_hash: str,
    tier: str,
    rollup: winpb.ElementRollup | None = None,
    raw: bytes | None = None,
    params: Mapping | None = None,
) -> pb.EntityTelemetryEvent | winpb.WindowedTelemetry | dict | bytes:
    """Build the output envelope. A sustainment output is a fresh EntityTelemetryEvent carrying only
    the derived sustainment + identity + provenance. No kinematics are
    echoed; consumers correlate by asset_id with the input stream. An anomaly
    output is a CloudEvent dict (see `_anomaly_event`)."""
    if out.kind == "latest_state":
        return _latest_state_event(src, sustainment, raw)
    if out.kind == "posture_transition":
        return _transition_event(src, sustainment, tier)
    if out.kind == "band_crossing":
        return _band_event(src, sustainment, params or {}, tier)
    if out.kind == "anomaly":
        return _anomaly_event(src, sustainment, out, unit=unit, version=version,
                              config_hash=config_hash, tier=tier)
    if out.kind == "window":
        return _window_event(src, sustainment, out, unit=unit, version=version,
                             config_hash=config_hash, tier=tier, rollup=rollup)
    evt = pb.EntityTelemetryEvent()
    evt.event_id = str(uuid.uuid4())
    evt.schema_revision = 1
    # Identity passthrough so downstream can correlate without joining.
    evt.asset.asset_id = src.asset.asset_id
    evt.asset.callsign = src.asset.callsign
    evt.asset.platform_type = src.asset.platform_type
    evt.asset.platform_variant = src.asset.platform_variant
    evt.asset.force = src.asset.force
    evt.sustainment.CopyFrom(sustainment)
    evt.provenance.producer_id = out.producer_id
    evt.provenance.source_protocol = out.source_protocol
    # source_sequence intentionally 0 — a unit is not a sequenced source.
    # sample_time reflects when the underlying SAMPLE was taken (the source
    # event's sample_time), not when the derivation ran. Carrying the source
    # time through keeps fusion's staleness check honest — otherwise every
    # derived event would look "fresh as of now" regardless of whether the
    # input feed has stopped.
    if src.provenance.sample_time.seconds or src.provenance.sample_time.nanos:
        evt.provenance.sample_time.CopyFrom(src.provenance.sample_time)
    else:
        evt.provenance.sample_time.GetCurrentTime()
    evt.provenance.ingest_time.GetCurrentTime()
    evt.provenance.classification = "U"
    # Origin-node provenance (ADR-0022 / ADR-0023). Inherit from the source
    # event if it carries them (raw-sensor-stream events do post-6a); fall
    # back to the env defaults of the faust-edge instance the unit runs
    # inside. Either way the derived event ends up edge-attributed.
    evt.provenance.edge_id = (
        src.provenance.edge_id or os.getenv("OPENDDIL_EDGE_ID", "edge-01")
    )
    evt.provenance.region_id = (
        src.provenance.region_id or os.getenv("OPENDDIL_REGION_ID", "region-01")
    )
    # Releasability labels (ADR-0029 §3). PURE PASSTHROUGH, and unlike
    # edge_id above there is NO ENV FALLBACK: the derivation engine has no
    # releasability declaration and must never acquire one. A derived
    # sustainment event is exactly as national as the telemetry it was
    # derived from, and if the source was unlabelled the derived event must
    # be too, so the §7 completeness gate can see it.
    #
    # THIS FUNCTION BUILDS A NEW MESSAGE FIELD BY FIELD, which is why this
    # line is needed at all. Anything not named here is dropped, silently,
    # and the drop is invisible at the seam: fusion's inbound event simply
    # has no label and fusion cannot tell "the source had none" from "a hop
    # lost it". Third instance of this shape in one night — cm-service's
    # _dict_to_record and its _recompute preservation block are the others.
    #
    # Found by running the pipeline, not by reading it: derived-sustainment
    # is fusion's HIGHEST-VOLUME inbound path, and asset_logistics_status sat
    # fully unlabelled while telemetry_latest_state and asset_cm_state were
    # already complete. Nothing errored. The propagation graph had one more
    # producer in it than the plan named.
    evt.provenance.originator_nation = src.provenance.originator_nation
    evt.provenance.releasable_to.extend(src.provenance.releasable_to)
    # Which unit, version, resolved parameters and tier made this value
    # (ADR-0034 addendum): a consumer can tell output the current
    # configuration produced from output it did not.
    evt.provenance.producer_stamp.unit = unit
    evt.provenance.producer_stamp.version = version
    evt.provenance.producer_stamp.config_hash = config_hash
    evt.provenance.producer_stamp.tier = tier
    return evt


def _latest_state_event(src: pb.EntityTelemetryEvent, payload: dict, raw: bytes | None) -> bytes:
    """No producer stamp: the bytes are what the old path sent. Untouched is the
    input bytes as they arrived; otherwise the posture fields are set on the
    parsed event (`src`, in place, as the old agent did) and it is re-serialized."""
    if payload.get("untouched"):
        if raw is None:
            raise OutputError("latest_state untouched needs the raw input bytes")
        return raw
    if payload["posture"] == "unspecified":
        src.operational_state.ClearField("posture_since")
    else:
        src.operational_state.posture_since.FromNanoseconds(int(round(payload["posture_since"] * 1e9)))
    src.operational_state.posture_status = _POSTURE_TO_PROTO[payload["posture"]]
    return src.SerializeToString()


def _band_event(env: dict, payload: dict, params: Mapping, tier: str) -> dict:
    """The element band CloudEvent, unchanged (no producer stamp). The host
    states what the unit may not: severity from the declared bands, the time
    from the envelope's observed_at_ns, and the labels from the envelope under
    the old type checks (a non-str nation is none, releasable_to keeps only
    its str members and is none unless a list)."""
    spec = (params.get("bands") or {}).get(payload["field"])
    if spec is None:
        raise OutputError(f"band_crossing field {payload['field']!r} is not in the declared bands")
    severity = spec["severity"].get(payload["to"])
    if severity is None:
        raise OutputError(f"band_crossing band {payload['to']!r} has no severity for {payload['field']!r}")
    nation = env.get("originator_nation") or ""
    releasable = env.get("releasable_to") or []
    if not isinstance(releasable, list):
        releasable = []
    releasable = [r for r in releasable if isinstance(r, str)]
    try:
        return build_band_event(
            env["asset_id"], payload["element_id"], payload["layer_name"], payload["field"],
            payload["from"], payload["to"], float(payload["value"]), env["observed_at_ns"] / 1e9,
            severity, nation if isinstance(nation, str) else "", releasable,
            tier, os.getenv("OPENDDIL_REGION_ID", "region-01"))
    except (ValueError, KeyError, OverflowError) as exc:
        raise OutputError(f"band crossing refused: {exc!r}") from exc


def _transition_event(src: pb.EntityTelemetryEvent, payload: dict, tier: str) -> dict:
    """The posture transition CloudEvent, unchanged (no producer stamp): its
    labels are copied from the source record, absent stays absent."""
    try:
        return build_transition_event(
            src.asset.asset_id, SimpleNamespace(**payload["prev"]), SimpleNamespace(**payload["new"]),
            payload["deploy_signal"], payload["speed"],
            src.provenance.originator_nation, list(src.provenance.releasable_to),
            tier, os.getenv("OPENDDIL_REGION_ID", "region-01"))
    except ValueError as exc:
        raise OutputError(f"posture transition refused: {exc}") from exc


def _window_event(
    src: pb.EntityTelemetryEvent,
    window: winpb.WindowedTelemetry,
    out: OutputDecl,
    *,
    unit: str,
    version: str,
    config_hash: str,
    tier: str,
    rollup: winpb.ElementRollup | None,
) -> winpb.WindowedTelemetry:
    """The WindowedTelemetry envelope, as the original window path built it:
    asset identity and variant from the source, `computed_at` and
    `provenance.ingest_time` the host wall clock, `sample_time` copied only
    when the source set it, the origin node from the env, and the source's
    releasability labels copied whole (ADR-0029 section 3: one asset's own
    samples, one author; no else-branch and no default, so an unlabelled
    source gives an unlabelled window for the gate to refuse). No
    `source_protocol`: the original window carried none. The element rollup the
    host holds for the asset, if any, rides along, because the element path
    writes the same key and neither producer may clear the other's part."""
    ev = winpb.WindowedTelemetry()
    ev.CopyFrom(window)
    now_ns = time.time_ns()
    ev.asset_id = src.asset.asset_id
    ev.platform_variant = src.asset.platform_variant or ""
    ev.computed_at.FromNanoseconds(now_ns)
    st = src.provenance.sample_time
    if st.seconds or st.nanos:
        ev.provenance.sample_time.CopyFrom(st)
    ev.provenance.producer_id = out.producer_id
    ev.provenance.edge_id = tier
    ev.provenance.region_id = os.getenv("OPENDDIL_REGION_ID", "region-01")
    ev.provenance.ingest_time.FromNanoseconds(now_ns)
    ev.provenance.classification = "U"
    if src.provenance.originator_nation:
        ev.provenance.originator_nation = src.provenance.originator_nation
    if src.provenance.releasable_to:
        ev.provenance.releasable_to.extend(src.provenance.releasable_to)
    ev.provenance.producer_stamp.unit = unit
    ev.provenance.producer_stamp.version = version
    ev.provenance.producer_stamp.config_hash = config_hash
    ev.provenance.producer_stamp.tier = tier
    if rollup is not None:
        ev.element_rollup.CopyFrom(rollup)
    return ev


def _anomaly_event(
    src: pb.EntityTelemetryEvent,
    payload: dict,
    out: OutputDecl,
    *,
    unit: str,
    version: str,
    config_hash: str,
    tier: str,
) -> dict:
    """The CloudEvent for tactical-events, in the shape the edge's own
    detection used (so the projector reads it unchanged) plus what a derived
    value now carries: provenance, the producer stamp and, from a labelled
    source, the releasability labels."""
    asset_id = src.asset.asset_id
    data = {
        "severity": out.severity,
        "summary": payload["summary"],
        "evidence": payload["evidence"],
        "edge_id": tier,
        "region_id": os.getenv("OPENDDIL_REGION_ID", "region-01"),
        # 0.0 is the ACCURATE value, as for sustainment: a fixed-threshold
        # rule has no oracle behind it, so there is nothing to be confident in.
        "provenance": {"origin": "DERIVED", "confidence": 0.0},
        "producer_stamp": {
            "unit": unit, "version": version, "config_hash": config_hash, "tier": tier,
        },
    }
    # Releasability (ADR-0029 section 3), copied only when the source was
    # labelled (posture_event.py's shape). No env fallback, as for
    # sustainment: an anomaly is exactly as national as the telemetry it
    # came from.
    if src.provenance.originator_nation:
        data["originator_nation"] = src.provenance.originator_nation
        data["releasable_to"] = list(src.provenance.releasable_to)
    return {
        "specversion": "1.0",
        "id": str(uuid.uuid4()),
        # ADR-0023 Faust-edge: source carries the edge id.
        "source": f"openddil/edge/{tier}/{asset_id}",
        "type": out.event_type,
        "subject": asset_id,
        "time": datetime.now(timezone.utc).isoformat(),
        "datacontenttype": "application/json",
        "data": data,
    }
