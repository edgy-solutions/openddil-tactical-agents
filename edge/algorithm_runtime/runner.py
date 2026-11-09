"""Faust wiring for declared units. The ONLY module here that imports faust.

`register(app)` loads the enabled units and, for each, builds its durable
state table and one agent on its input topic. The agent is separate from
faust_edge.py's own agents: its own consumer, its own table, its own output
topic. That separation is the input/output seam (ADR-0020): a unit does not
reach into faust-edge's element buffers, and
faust-edge does not see a unit's state. The call in faust_edge.py is the only
coupling; to extract the runner into its own service, delete that import and
call and give the new service its own `faust.App`.

Everything that can be wrong with the deployment is refused here, at startup,
with the fix named. A runner that started on defaults after being handed a
setting it no longer reads would produce plausible wrong numbers.
"""
from __future__ import annotations

import copy
import json
import logging
import os
from pathlib import Path

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, declaration, loader, outputs
from algorithm_runtime.contract import Unit

log = logging.getLogger("algorithm_runtime")

_COUNT_LOG_EVERY = 1000


class StartupRefusal(RuntimeError):
    pass


# The old in-line paths' env names, and the (unit, param) each became.
_LEGACY_UNIT_ENV = {
    "FLUID_WINDOW_MIN": ("trends", "fluid_window_min"),
    "WEAR_WINDOW_MIN": ("trends", "wear_window_min"),
    "WINDOW_EMIT_EVERY_N": ("trends", "emit_every_n_samples"),
    "WINDOW_DEQUE_CAP": ("trends", "window_cap"),
    "POSTURE_MOVE_SPEED_MPS": ("posture", "move_speed_mps"),
    "POSTURE_MOVE_HOLD_S": ("posture", "move_hold_s"),
    "POSTURE_STOP_HOLD_S": ("posture", "stop_hold_s"),
    "ELEMENT_BANDS_JSON": ("element_bands", "bands"),
}


def _refuse_legacy_env() -> None:
    legacy = sorted(k for k in os.environ if k.startswith("PROGNOSTICS_"))
    if legacy:
        k = legacy[0]
        param = k[len("PROGNOSTICS_"):].lower()
        raise StartupRefusal(
            f"{k} is set but is no longer read. Parameters moved to ALGORITHM_PARAMS_JSON: "
            f'set {{"wear": {{"{param}": <value>}}}} and unset {k} '
            f"(all set: {', '.join(legacy)})"
        )
    old = sorted(k for k in _LEGACY_UNIT_ENV if k in os.environ)
    if old:
        k = old[0]
        unit, param = _LEGACY_UNIT_ENV[k]
        raise StartupRefusal(
            f"{k} is set but is no longer read. Parameters moved to ALGORITHM_PARAMS_JSON: "
            f'set {{"{unit}": {{"{param}": <value>}}}} and unset {k} '
            f"(all set: {', '.join(old)})"
        )


def make_record_class(decl: declaration.Declaration) -> type:
    """The faust Record for a unit's state, from its declared fields in
    order. The namespace is the declared one so an existing changelog's
    records load into it unchanged."""
    annotations, body = {}, {}
    for f in decl.state_fields:
        if f.persistence == "memory":
            continue          # not in the record: the runner holds it per asset
        base = {"number": float, "integer": int, "string": str}[f.type]
        annotations[f.name] = (base | None) if f.nullable else base
        body[f.name] = f.default
    body["__annotations__"] = annotations
    name = decl.state_record_namespace.rsplit(".", 1)[-1]
    return type(name, (faust.Record,), body,
                serializer="json", namespace=decl.state_record_namespace)


def _count(counters: dict, new) -> None:
    for k, n in new.items():
        before = counters.get(k, 0)
        counters[k] = before + n
        if before == 0 or counters[k] // _COUNT_LOG_EVERY > before // _COUNT_LOG_EVERY:
            log.warning("algorithm_runtime counter %s=%d", k, counters[k])


class Wiring:
    """One unit's per-record path, minus the broker: what the agent does for
    each message, so it runs without Faust.

    A memory-persistence unit's state lives in `memory`, a per-process dict
    keyed by asset_id: a restart of the process starts it empty, as the old
    in-line buffers did. A durable unit's state is the Table record the agent
    passes in."""

    def __init__(self, unit: Unit, decl: declaration.Declaration, window_rollup, last_window: dict):
        self.unit, self.decl = unit, decl
        self.window_rollup = window_rollup
        self.last_window = last_window
        self.memory: dict[str, dict] = {}
        self.counters: dict = {}
        # Memory-persistence fields of a durable unit: asset_id -> {field: value}.
        self.field_names = [f.name for f in decl.state_fields if f.persistence == "memory"]
        self.field_memory: dict[str, dict] = {}

    def _remember(self, asset_id: str, kind: str, payload) -> None:
        if kind == "window":
            self.last_window[asset_id] = payload

    def process(self, raw: bytes, record_time_ns: int | None, state: dict | None = None):
        """(new_state or None, sends). `state` is the durable record's fields
        (None for none); a memory unit's is looked up here and kept here."""
        memory = self.decl.state_persistence == "memory"
        mixed = not memory and bool(self.field_names)
        extra = {}
        if self.decl.input_format == "json-envelope":
            # Parse the envelope once: the asset key and the gates share it.
            env = core.parse_envelope(raw)
            aid = env.get("asset_id") if env is not None else None
            asset = aid if isinstance(aid, str) else ""
            extra["parsed"] = env
        else:
            asset = _asset_key(raw) if memory or mixed else ""
        if memory:
            state = self.memory.get(asset) if asset else None
        elif mixed and state is not None and asset:
            held = self.field_memory.get(asset)
            if held is None:
                held = {f.name: copy.deepcopy(f.default) for f in self.decl.state_fields
                        if f.persistence == "memory"}
            state = {**state, **held}
        new_state, sends, new_counters = core.process_one(
            self.unit, self.decl, raw, state, record_time_ns,
            window_rollup=self.window_rollup, on_payload=self._remember, **extra)
        _count(self.counters, new_counters)
        if memory and new_state is not None:
            self.memory[asset] = new_state
        elif mixed and new_state is not None and asset:
            self.field_memory[asset] = {n: new_state[n] for n in self.field_names}
            new_state = {k: v for k, v in new_state.items() if k not in self.field_names}
        return new_state, sends


class Runner:
    """What `register` hands back: the wired units and what the host reads
    from them."""

    def __init__(self):
        self.wirings: dict[str, Wiring] = {}
        self._last_window: dict[str, dict] = {}

    def last_window(self, asset_id: str):
        """The last `window` payload (the unit's dict) emitted for the asset,
        or None. The element path builds its window from it."""
        return self._last_window.get(asset_id)


def record_time_ns(message) -> int | None:
    """The Faust message's broker timestamp (seconds, float) as ns, ms * 1e6."""
    ts = getattr(message, "timestamp", None)
    return None if ts is None else int(round(ts * 1000)) * 1_000_000


def register(app: faust.App, window_rollup=None) -> Runner:
    """Register every enabled unit into the host Faust app.

    Per unit adds one `@app.agent` consumer of its input topic, one durable
    Table (RocksDB-backed, changelog-replicated, asset_id-keyed — ADR-0022
    compliant) unless the unit declares memory state, and production to its
    declared output topic. Call once, at faust_edge.py module load: a second
    call on the same app would register duplicate agents and tables.

    `window_rollup(asset_id)` is the host's hook for `window` outputs: it
    returns the element rollup to attach, or None. Returns the `Runner`."""
    enabled_raw = os.environ.get("ALGORITHMS_ENABLED", "").strip()
    if not enabled_raw:
        raise StartupRefusal(
            "ALGORITHMS_ENABLED is unset: name the units to run, e.g. ALGORITHMS_ENABLED=wear,thermal")
    enabled = [u.strip() for u in enabled_raw.split(",") if u.strip()]
    _refuse_legacy_env()

    algorithms_dir = os.environ.get("ALGORITHMS_DIR", "")
    if not algorithms_dir or not Path(algorithms_dir).is_dir():
        raise StartupRefusal(
            f"ALGORITHMS_DIR={algorithms_dir!r} is not a directory: point it at the directory "
            "holding the unit directories (the runtime bundle mounts it)")
    tier = os.environ.get("OPENDDIL_EDGE_ID", "")
    if not tier:
        raise StartupRefusal("OPENDDIL_EDGE_ID is unset: every output is stamped with its tier")
    if "producer_stamp" not in pb.Provenance.DESCRIPTOR.fields_by_name:
        raise StartupRefusal(
            "the loaded openddil.telemetry bindings lack Provenance.producer_stamp: "
            "regenerate them from an openddil-contracts that has it")

    try:
        params_json = json.loads(os.environ.get("ALGORITHM_PARAMS_JSON") or "{}")
    except json.JSONDecodeError as exc:
        raise StartupRefusal(f"ALGORITHM_PARAMS_JSON is not valid JSON: {exc}") from exc
    if not isinstance(params_json, dict):
        raise StartupRefusal('ALGORITHM_PARAMS_JSON must be an object like {"wear": {...}}')
    unknown = sorted(set(params_json) - set(enabled))
    if unknown:
        raise StartupRefusal(
            f"ALGORITHM_PARAMS_JSON names {unknown}, not in ALGORITHMS_ENABLED={enabled}: "
            "enable the unit or remove its parameters")

    loaded, tables = [], {}
    for name in enabled:
        unit_dir = Path(algorithms_dir) / name
        if not unit_dir.is_dir():
            raise StartupRefusal(f"unit directory {unit_dir} is missing: ALGORITHMS_ENABLED names {name!r}")
        decl = declaration.load(unit_dir / "unit.yaml")
        if decl.unit != name:
            raise StartupRefusal(f"{unit_dir}: declares unit {decl.unit!r}, expected {name!r}")
        if decl.state_table is not None:
            if decl.state_table in tables:
                raise StartupRefusal(
                    f"units {tables[decl.state_table]!r} and {name!r} both claim table {decl.state_table!r}")
            tables[decl.state_table] = name
        if any(o.kind == "window" for o in decl.outputs):
            if "window" in tables:
                raise StartupRefusal(
                    f"units {tables['window']!r} and {name!r} both emit window outputs: "
                    "one writer per key on the windows topic")
            tables["window"] = name
        params = declaration.resolve_params(decl, params_json.get(name))
        unit = loader.load_unit_class(unit_dir, decl, StartupRefusal)(params)
        loaded.append((decl, unit, params))

    runner = Runner()
    for decl, unit, params in loaded:
        _wire(app, decl, unit, params, runner, window_rollup)
    return runner


def _wire(app: faust.App, decl: declaration.Declaration, unit: Unit, params: dict,
          runner: Runner, window_rollup) -> None:
    durable = decl.state_persistence == "durable"
    in_topic = app.topic(decl.input_topic, value_type=bytes)
    out_topics = {o.topic: app.topic(o.topic, value_type=bytes) for o in decl.outputs}
    if durable:
        record_cls = make_record_class(decl)
        table = app.Table(
            decl.state_table,
            default=record_cls,
            # MUST match the input topic's partition count. See
            # openddil-tactical-agents/README.md "Faust Tables — the
            # partition-count invariant" — the rule is strict-but-invisible
            # (Faust only enforces strictly when the app has >1 Table). If
            # you re-partition the source for parallelism, every Table in
            # the app must change together — no per-Table autonomy.
            partitions=decl.state_partitions,
        )
    log.info(
        "algorithm unit registered: unit=%s version=%s config_hash=%s params=%s persistence=%s",
        decl.unit, decl.version, declaration.config_hash(decl.unit, decl.version, params), params,
        decl.state_persistence,
    )
    wiring = Wiring(unit, decl, window_rollup, runner._last_window)
    runner.wirings[decl.unit] = wiring
    names = [f.name for f in decl.state_fields if f.persistence != "memory"]

    @app.agent(in_topic, name=f"algorithm.{decl.unit}")
    async def algorithm_process(stream):
        async for raw in stream:
            event = faust.current_event()
            rt_ns = record_time_ns(event.message) if event is not None else None
            if durable:
                asset_key = _asset_key(raw)
                rec = table[asset_key] if asset_key else None
                state = {n: getattr(rec, n) for n in names} if rec is not None else None
                new_state, sends = wiring.process(raw, rt_ns, state)
                if new_state is None:
                    continue
                table[asset_key] = record_cls(**new_state)
            else:
                new_state, sends = wiring.process(raw, rt_ns)
            for topic, key, msg in sends:
                # The topic is whichever the declaration names; the manifest
                # lists each one a declared unit writes (one note per topic).
                await out_topics[topic].send(  # produces: derived-sustainment
                    key=key,                   # produces: tactical-events
                    # produces: telemetry-latest-state
                    value=outputs.encode(msg))  # produces: asset-telemetry-windows


def _asset_key(raw: bytes) -> str:
    """asset_id of a raw message, or "" if it will not parse (core skips
    those and counts them)."""
    try:
        evt = pb.EntityTelemetryEvent()
        evt.ParseFromString(raw)
    except Exception:  # noqa: BLE001
        return ""
    return evt.asset.asset_id
