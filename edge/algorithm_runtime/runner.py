"""Faust wiring for declared units. The ONLY module here that imports faust.

`register(app)` loads the enabled units and, for each, builds its durable
state table and one agent on its input topic. The agent is separate from
faust_edge.py's own agents: its own consumer, its own table, its own output
topic. That separation is the input/output seam (ADR-0020): a unit does not
reach into faust-edge's window buffers or `asset_state` table, and
faust-edge does not see a unit's state. The call in faust_edge.py is the only
coupling; to extract the runner into its own service, delete that import and
call and give the new service its own `faust.App`.

Everything that can be wrong with the deployment is refused here, at startup,
with the fix named. A runner that started on defaults after being handed a
setting it no longer reads would produce plausible wrong numbers.
"""
from __future__ import annotations

import importlib.util
import json
import logging
import os
import sys
from pathlib import Path

import faust
from openddil.telemetry.v1 import telemetry_pb2 as pb

from algorithm_runtime import core, declaration, guard
from algorithm_runtime.contract import Unit

log = logging.getLogger("algorithm_runtime")

_COUNT_LOG_EVERY = 1000


class StartupRefusal(RuntimeError):
    pass


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


def _load_unit_class(unit_dir: Path, decl: declaration.Declaration) -> type:
    guard.check_unit_dir(unit_dir)
    file_name, class_name = decl.entrypoint.split(":")
    path = unit_dir / file_name
    if not path.is_file():
        raise StartupRefusal(f"{decl.unit}: entrypoint file {path} does not exist")
    name = f"openddil_algorithms.{decl.unit}"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolves its own module through here
    spec.loader.exec_module(module)
    cls = getattr(module, class_name, None)
    if not (isinstance(cls, type) and issubclass(cls, Unit)):
        raise StartupRefusal(f"{decl.unit}: {decl.entrypoint} is not a subclass of contract.Unit")
    return cls


def make_record_class(decl: declaration.Declaration) -> type:
    """The faust Record for a unit's state, from its declared fields in
    order. The namespace is the declared one so an existing changelog's
    records load into it unchanged."""
    annotations, body = {}, {}
    for f in decl.state_fields:
        base = float if f.type == "number" else int
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


def register(app: faust.App) -> None:
    """Register every enabled unit into the host Faust app.

    Per unit adds one `@app.agent` consumer of its input topic, one durable
    Table (RocksDB-backed, changelog-replicated, asset_id-keyed — ADR-0022
    compliant), and production to its declared output topic. Call once, at
    faust_edge.py module load: a second call on the same app would register
    duplicate agents and tables."""
    enabled_raw = os.environ.get("ALGORITHMS_ENABLED", "").strip()
    if not enabled_raw:
        raise StartupRefusal(
            "ALGORITHMS_ENABLED is unset: name the units to run, e.g. ALGORITHMS_ENABLED=wear")
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
        if decl.state_table in tables:
            raise StartupRefusal(
                f"units {tables[decl.state_table]!r} and {name!r} both claim table {decl.state_table!r}")
        tables[decl.state_table] = name
        params = declaration.resolve_params(decl, params_json.get(name))
        unit = _load_unit_class(unit_dir, decl)(params)
        loaded.append((decl, unit, params))

    for decl, unit, params in loaded:
        _wire(app, decl, unit, params)


def _wire(app: faust.App, decl: declaration.Declaration, unit: Unit, params: dict) -> None:
    record_cls = make_record_class(decl)
    in_topic = app.topic(decl.input_topic, value_type=bytes)
    out_topics = {o.topic: app.topic(o.topic, value_type=bytes) for o in decl.outputs}
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
        "algorithm unit registered: unit=%s version=%s config_hash=%s params=%s",
        decl.unit, decl.version, declaration.config_hash(decl.unit, decl.version, params), params,
    )
    counters: dict = {}
    names = [f.name for f in decl.state_fields]

    @app.agent(in_topic, name=f"algorithm.{decl.unit}")
    async def algorithm_process(stream):
        async for raw in stream:
            asset_key = _asset_key(raw)
            rec = table[asset_key] if asset_key else None
            state = {n: getattr(rec, n) for n in names} if rec is not None else None
            new_state, sends, new_counters = core.process_one(unit, decl, raw, state)
            _count(counters, new_counters)
            if new_state is None:
                continue
            table[asset_key] = record_cls(**new_state)
            for topic, key, msg in sends:
                await out_topics[topic].send(  # produces: derived-sustainment
                    key=key, value=msg.SerializeToString())


def _asset_key(raw: bytes) -> str:
    """asset_id of a raw message, or "" if it will not parse (core skips
    those and counts them)."""
    try:
        evt = pb.EntityTelemetryEvent()
        evt.ParseFromString(raw)
    except Exception:  # noqa: BLE001
        return ""
    return evt.asset.asset_id
