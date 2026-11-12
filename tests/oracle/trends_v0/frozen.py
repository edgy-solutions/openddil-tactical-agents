# FROZEN: the rolling-window path of edge/faust_edge.py at 26a8168 (`_buffer_event`, `_emit_window_for_asset`,
# `_build_window`, `_window_from_envelope`, and step 1a of `process`), minus faust, test oracle only.
# Imported by test_trends_equivalence.py and by the replay harness's --compare-oracle, never edited.
# Each method below is that file's code verbatim, with the module globals it read (window_buffers,
# element_rollups, the window constants, the edge and region ids) held on the instance, and the clock
# (`time.time()`) passed in. The one other change is marked CHANGED: the ammo push built a pint
# UnitRegistry per push, and here one registry is shared.
from __future__ import annotations

from collections import deque

from pint import UnitRegistry

from openddil.logistics.v1 import windowed_telemetry_pb2 as winpb
from openddil.telemetry.v1 import telemetry_pb2 as pb

from detection.windows import (
    Sample,
    build_window_spec,
    compute_trend,
    proto_to_pint,
    trend_to_proto,
)


class OldTrends:
    """The old per-process window state and its two producers."""

    def __init__(self, *, fluid_window_ns: int, wear_window_ns: int, emit_every_n: int,
                 deque_cap: int, edge_id: str, region_id: str):
        self.fluid_window_ns = fluid_window_ns
        self.wear_window_ns = wear_window_ns
        self.emit_every_n = emit_every_n
        self.deque_cap = deque_cap
        self.edge_id = edge_id
        self.region_id = region_id
        self.window_buffers: dict[str, dict[str, deque]] = {}
        self.samples_since_last_emit: dict[str, int] = {}
        self.element_rollups: dict[str, winpb.ElementRollup] = {}
        self.ureg = UnitRegistry()

    def step_1a(self, evt: pb.EntityTelemetryEvent, now_ns: int) -> winpb.WindowedTelemetry | None:
        """Step 1a of `process`, with `now_ns` given instead of read from the clock;
        returns the window it would have sent."""
        aid = evt.asset.asset_id
        self._buffer_event(evt, now_ns)
        if aid:
            self.samples_since_last_emit[aid] = self.samples_since_last_emit.get(aid, 0) + 1
            if self.samples_since_last_emit[aid] >= self.emit_every_n:
                self.samples_since_last_emit[aid] = 0
                w = self._emit_window_for_asset(evt, now_ns)
                if w is not None:
                    return w
        return None

    def _emit_window_for_asset(self, evt: pb.EntityTelemetryEvent,
                                now_ns: int) -> winpb.WindowedTelemetry | None:
        """Build a WindowedTelemetry from the buffered samples for one asset.
        Returns None if there isn't enough data yet (no trend possible)."""
        aid = evt.asset.asset_id
        if not self.window_buffers.get(aid):
            return None
        return self._build_window(
            aid,
            evt.asset.platform_variant or "",
            now_ns,
            sample_time=evt.provenance.sample_time,
            originator_nation=evt.provenance.originator_nation,
            releasable_to=evt.provenance.releasable_to,
            evt=evt,
        )

    def _build_window(self, aid: str,
                      platform_variant: str,
                      now_ns: int,
                      *,
                      sample_time=None,
                      originator_nation: str = "",
                      releasable_to=(),
                      evt: pb.EntityTelemetryEvent | None = None,
                      window_range_ns: tuple[int, int] | None = None,
                      ) -> winpb.WindowedTelemetry:
        """Shared window construction for both producers: the raw path (which
        passes the source event, for capacity and fault passthrough) and the
        element path (which passes only the envelope's labels and a fixed range).
        Carries whatever sustainment trends this asset's buffers already hold and
        the cached element rollup, if any."""
        buffers = self.window_buffers.get(aid, {})

        out = winpb.WindowedTelemetry()
        out.asset_id = aid
        out.platform_variant = platform_variant
        out.computed_at.FromNanoseconds(now_ns)
        # ADR-0023 Phase 6b §A.2: stamp origin-node provenance from this
        # faust-edge instance's env (faust-edge is already per-edge). The
        # projector telemetry_windows handler reads this with env-default
        # fallback; faust-regional's region-wear-trends aggregator (§B)
        # consumes it as an attributed input. Inherit producer_id/sample_time
        # from the source event when possible.
        if sample_time is not None and (sample_time.seconds or sample_time.nanos):
            out.provenance.sample_time.CopyFrom(sample_time)
        out.provenance.producer_id = "faust-edge"
        out.provenance.edge_id = self.edge_id
        out.provenance.region_id = self.region_id
        out.provenance.ingest_time.FromNanoseconds(now_ns)
        out.provenance.classification = "U"

        # ADR-0029 §3: PROPAGATE, do not derive. A window is a rollup of ONE
        # asset's own samples, so it inherits that asset's labels whole -- this
        # is not the aggregate case the regional aggregator handles, where rows
        # from several authors are combined and no one of them may claim
        # authorship of the result. Here there is exactly one author and it is
        # the same author as the source event.
        #
        # WITHOUT THIS COPY the windowed path is lossy, and lossy in the
        # direction that reads as correct: an asset reaching fusion only through
        # `asset-telemetry-windows` would arrive unlabelled and be refused at
        # the egress gate as `unlabelled` -- indistinguishable from an asset
        # nobody declared, with the fix appearing to belong at an ingress that
        # had in fact done its job. It did not bite because raw-sensor-stream is
        # also a direct fusion input and fusion's stored label is sticky, so one
        # labelled inbound was enough to mask it.
        #
        # No else-branch and no default. An unlabelled source event produces an
        # unlabelled window, which the gate then refuses; inventing a value here
        # would hide the thing the gate exists to surface.
        if originator_nation:
            out.provenance.originator_nation = originator_nation
        if releasable_to:
            out.provenance.releasable_to.extend(releasable_to)

        total_samples = 0
        window_min_start: int | None = None

        # Fluid trends (fuel and any future named fluid)
        for key, dq in buffers.items():
            if not key.startswith("fluid:"):
                continue
            signal = key.split(":", 1)[1]
            trend = compute_trend(list(dq), window_ns=self.fluid_window_ns, now_ns=now_ns)
            if trend is None:
                continue
            out.fluid_trends[signal].CopyFrom(trend_to_proto(trend))
            total_samples += trend.sample_count
            if window_min_start is None or trend.window_start_ns < window_min_start:
                window_min_start = trend.window_start_ns

        # Consumable trends — group by slot key
        ammo_slots: dict[str, dict] = {}
        for key, dq in buffers.items():
            if not key.startswith("ammo:"):
                continue
            slot = key.split(":", 1)[1]
            trend = compute_trend(list(dq), window_ns=self.fluid_window_ns, now_ns=now_ns)
            if trend is None:
                continue
            ammo_slots[slot] = {"trend": trend}
        # Capacity passthrough from the latest event
        for slot, info in ammo_slots.items():
            cs = (evt.sustainment.consumables.items.get(slot)
                  if evt is not None else None)
            t = out.consumable_trends.add()
            t.slot_key = slot
            t.remaining.CopyFrom(trend_to_proto(info["trend"]))
            if cs is not None:
                t.capacity = cs.quantity_capacity
                t.nsn = cs.nsn

        # Wear trends — pair hours_in_service with remaining_useful_life per component
        component_names = set()
        for key in buffers:
            if key.startswith("wear_hours:"):
                component_names.add(key.split(":", 1)[1])
        for comp in component_names:
            hours_buf = buffers.get(f"wear_hours:{comp}")
            rul_buf   = buffers.get(f"wear_rul:{comp}")
            if hours_buf is None or rul_buf is None:
                continue
            h_trend = compute_trend(list(hours_buf),
                                     window_ns=self.wear_window_ns, now_ns=now_ns)
            r_trend = compute_trend(list(rul_buf),
                                     window_ns=self.wear_window_ns, now_ns=now_ns)
            if h_trend is None and r_trend is None:
                continue
            t = out.wear_trends.add()
            t.component_key = comp
            if h_trend is not None:
                t.hours_in_service.CopyFrom(trend_to_proto(h_trend))
            if r_trend is not None:
                t.remaining_useful_life.CopyFrom(trend_to_proto(r_trend))

        # Latest subsystem-fault tokens (passthrough — not aggregated)
        if evt is not None and list(evt.sustainment.health.active_fault_codes):
            out.active_fault_codes_latest.extend(
                list(evt.sustainment.health.active_fault_codes),
            )

        # WindowSpec — use the widest range we covered.
        if window_range_ns is not None:
            start_ns, end_ns = window_range_ns
        else:
            start_ns, end_ns = window_min_start or now_ns, now_ns
        out.window.CopyFrom(build_window_spec(
            window_start_ns=start_ns,
            window_end_ns=end_ns,
            sample_count=total_samples,
        ))

        rollup = self.element_rollups.get(aid)
        if rollup is not None:
            out.element_rollup.CopyFrom(rollup)
        return out

    def _window_from_envelope(self, env: dict,
                              rollup: winpb.ElementRollup,
                              now_ns: int) -> winpb.WindowedTelemetry:
        """Window for the element path: the window spans the snapshot's own
        observation instant, and labels come from the envelope (ADR-0029
        PROPAGATE, no default -- see _build_window). The rollup is attached from
        the cache, so the caller stores it there first."""
        aid = env.get("asset_id") or ""
        obs_ns = (rollup.observed_at.ToNanoseconds()
                  if rollup.HasField("observed_at") else now_ns)
        releasable = env.get("releasable_to") or []
        if not isinstance(releasable, list):
            releasable = []
        nation = env.get("originator_nation") or ""
        return self._build_window(
            aid,
            env.get("platform_variant") or "",
            now_ns,
            originator_nation=nation if isinstance(nation, str) else "",
            releasable_to=[r for r in releasable if isinstance(r, str)],
            window_range_ns=(obs_ns, obs_ns),
        )

    def _buffer_event(self, evt: pb.EntityTelemetryEvent, now_ns: int) -> None:
        """Add the event's sustainment quantities to the per-asset window buffers."""
        aid = evt.asset.asset_id
        if not aid:
            return
        buffers = self.window_buffers.setdefault(aid, {})

        def _push(signal_key: str, proto_qty) -> None:
            pq = proto_to_pint(proto_qty)
            if pq is None:
                return
            dq = buffers.setdefault(signal_key, deque(maxlen=self.deque_cap))
            dq.append(Sample(epoch_ns=now_ns, quantity=pq))

        _push("fluid:fuel_remaining", evt.sustainment.fluids.fuel_remaining)

        for slot, state in evt.sustainment.consumables.items.items():
            if state.quantity_capacity > 0:
                # Convert remaining count to a dimensionless quantity so the
                # regression has consistent units.
                ureg = self.ureg  # CHANGED: one shared registry; the original built one per push
                dq = buffers.setdefault(f"ammo:{slot}", deque(maxlen=self.deque_cap))
                dq.append(Sample(
                    epoch_ns=now_ns,
                    quantity=ureg.Quantity(float(state.quantity_remaining), "count"),
                ))

        for comp, state in evt.sustainment.wear.components.items():
            _push(f"wear_hours:{comp}", state.hours_in_service)
            _push(f"wear_rul:{comp}",   state.remaining_useful_life)
