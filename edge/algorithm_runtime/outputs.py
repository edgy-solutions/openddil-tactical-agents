"""Output kinds -> proto envelopes.

A unit returns a payload dict per output kind; this module turns it into the
message on the wire. The source class of every value a unit emits (DERIVED,
confidence 0.0) is the contract's to state, never the unit's, so a payload
that tries to carry its own `value_provenance` is refused.
"""
from __future__ import annotations

import os
import uuid
from typing import Mapping

from google.protobuf import json_format
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime.declaration import OutputDecl


class OutputError(ValueError):
    pass


def build_payload(kind: str, payload: Mapping) -> pb.SustainmentMetrics:
    """Typed message for one output kind."""
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


def build_event(
    src: pb.EntityTelemetryEvent,
    sustainment: pb.SustainmentMetrics,
    out: OutputDecl,
    *,
    unit: str,
    version: str,
    config_hash: str,
    tier: str,
) -> pb.EntityTelemetryEvent:
    """Build the output envelope: a fresh EntityTelemetryEvent carrying only
    the derived sustainment + identity + provenance. No kinematics are
    echoed; consumers correlate by asset_id with the input stream."""
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
