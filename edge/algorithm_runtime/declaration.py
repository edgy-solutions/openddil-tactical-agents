"""Load and validate a unit's YAML declaration.

Every refusal is a `DeclarationError` naming the key at fault: a bad
declaration must stop the process at startup, not surface as a wrong number
in a run. The same rules are written for tooling as
`algorithms/declaration.schema.json`; a test keeps the two in agreement, with
one exception the schema language cannot state (a default outside its own
minimum/maximum).

Two rules are ADR-0034's "make the invalid configuration unwritable"
moves: the only accepted `runtime` is stream-parallel, and a `terminal`
composition with an `upward` output is refused (a lossy result must not be
emittable up the tier tree).
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import yaml

from algorithm_runtime.contract import ROLES

API_VERSION = "openddil.io/algorithm/v1"
RUNTIME = "stream-parallel"
LABELS = "copy-source"
# Output kind table. Each kind maps to an envelope field in outputs.py.
OUTPUT_KINDS = ("sustainment",)
COMPOSITIONS = ("propagating", "terminal")
DIRECTIONS = ("local", "upward")
PARAM_TYPES = ("number", "integer")

_TOP_KEYS = {
    "apiVersion", "unit", "version", "runtime", "entrypoint", "key",
    "composition", "inputs", "params", "state", "outputs", "resources",
    "dependencies",
}
_REQUIRED_TOP = _TOP_KEYS - {"dependencies"}


class DeclarationError(ValueError):
    pass


@dataclass(frozen=True)
class Param:
    name: str
    type: str
    default: int | float
    minimum: float | None = None
    maximum: float | None = None
    unit: str | None = None


@dataclass(frozen=True)
class StateField:
    name: str
    type: str
    default: int | float | None
    nullable: bool = False


@dataclass(frozen=True)
class InputRole:
    role: str
    required: bool = False


@dataclass(frozen=True)
class OutputDecl:
    kind: str
    topic: str
    direction: str
    producer_id: str
    source_protocol: str
    labels: str


@dataclass(frozen=True)
class Declaration:
    unit: str
    version: str
    entrypoint: str
    key: str
    composition: str
    input_topic: str
    roles: tuple[InputRole, ...]
    params: tuple[Param, ...]
    state_table: str
    state_partitions: int
    state_record_namespace: str
    state_fields: tuple[StateField, ...]
    outputs: tuple[OutputDecl, ...]
    memory_mi: float
    cpu_m: float

    def param(self, name: str) -> Param | None:
        return next((p for p in self.params if p.name == name), None)


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _coerce(value: Any, typ: str, where: str) -> int | float:
    """Typed value for a declared number/integer, or refuse. Integers are
    accepted for number and coerced with float(); numbers are refused for
    integer unless integral. Strings and booleans are never accepted."""
    if not _is_num(value):
        raise DeclarationError(f"{where}: {value!r} is not a {typ}")
    if typ == "number":
        return float(value)
    if isinstance(value, float):
        if not value.is_integer():
            raise DeclarationError(f"{where}: {value!r} is not an integer")
        return int(value)
    return value


def _mapping(v: Any, where: str, allowed: set[str], required: set[str]) -> Mapping:
    if not isinstance(v, dict):
        raise DeclarationError(f"{where}: must be a mapping")
    for k in v:
        if k not in allowed:
            raise DeclarationError(f"{where}: unknown key {k!r}")
    for k in sorted(required):
        if k not in v:
            raise DeclarationError(f"{where}: missing key {k!r}")
    return v


def _string(v: Any, where: str) -> str:
    if not isinstance(v, str) or not v:
        raise DeclarationError(f"{where}: must be a non-empty string")
    return v


def validate(raw: Any) -> Declaration:
    d = _mapping(raw, "declaration", _TOP_KEYS, _REQUIRED_TOP)

    if d["apiVersion"] != API_VERSION:
        raise DeclarationError(f"apiVersion: {d['apiVersion']!r} is not {API_VERSION!r}")
    if d["runtime"] != RUNTIME:
        raise DeclarationError(
            f"runtime: {d['runtime']!r} is not {RUNTIME!r}, the only class this runner accepts")
    unit = _string(d["unit"], "unit")
    if not (unit[0].islower() and all(c.islower() or c.isdigit() or c == "_" for c in unit)):
        raise DeclarationError(f"unit: {unit!r} must match [a-z][a-z0-9_]*")
    version = _string(d["version"], "version")
    entrypoint = _string(d["entrypoint"], "entrypoint")
    if entrypoint.count(":") != 1 or not all(entrypoint.split(":")):
        raise DeclarationError(f"entrypoint: {entrypoint!r} must be <file>.py:<Class>")
    if d["key"] != "asset_id":
        raise DeclarationError(
            f"key: {d['key']!r} is not 'asset_id', the only key this runner partitions by")
    composition = d["composition"]
    if composition not in COMPOSITIONS:
        raise DeclarationError(f"composition: {composition!r} is not one of {list(COMPOSITIONS)}")

    if d.get("dependencies", []) != []:
        raise DeclarationError(
            "dependencies: must be empty; dependencies ship in the image, never fetched")

    inp = _mapping(d["inputs"], "inputs", {"topic", "roles"}, {"topic", "roles"})
    input_topic = _string(inp["topic"], "inputs.topic")
    if not isinstance(inp["roles"], list) or not inp["roles"]:
        raise DeclarationError("inputs.roles: must be a non-empty list")
    roles = []
    for i, r in enumerate(inp["roles"]):
        r = _mapping(r, f"inputs.roles[{i}]", {"role", "required"}, {"role"})
        if r["role"] not in ROLES:
            raise DeclarationError(
                f"inputs.roles[{i}].role: unknown role {r['role']!r}; known: {sorted(ROLES)}")
        req = r.get("required", False)
        if not isinstance(req, bool):
            raise DeclarationError(f"inputs.roles[{i}].required: must be a boolean")
        roles.append(InputRole(r["role"], req))

    params = []
    if not isinstance(d["params"], dict):
        raise DeclarationError("params: must be a mapping")
    for name, p in d["params"].items():
        w = f"params.{name}"
        p = _mapping(p, w, {"type", "default", "minimum", "maximum", "unit"}, {"type", "default"})
        if p["type"] not in PARAM_TYPES:
            raise DeclarationError(f"{w}.type: {p['type']!r} is not one of {list(PARAM_TYPES)}")
        default = _coerce(p["default"], p["type"], f"{w}.default")
        lo, hi = p.get("minimum"), p.get("maximum")
        for bound, label in ((lo, "minimum"), (hi, "maximum")):
            if bound is not None and not _is_num(bound):
                raise DeclarationError(f"{w}.{label}: must be a number")
        if lo is not None and default < lo:
            raise DeclarationError(f"{w}.default: {default!r} is below minimum {lo!r}")
        if hi is not None and default > hi:
            raise DeclarationError(f"{w}.default: {default!r} is above maximum {hi!r}")
        if "unit" in p and not isinstance(p["unit"], str):
            raise DeclarationError(f"{w}.unit: must be a string")
        params.append(Param(name, p["type"], default, lo, hi, p.get("unit")))

    st = _mapping(d["state"], "state", {"table", "partitions", "record_namespace", "fields"},
                  {"table", "partitions", "record_namespace", "fields"})
    table = _string(st["table"], "state.table")
    parts = st["partitions"]
    if not isinstance(parts, int) or isinstance(parts, bool) or parts < 1:
        raise DeclarationError("state.partitions: must be an integer >= 1")
    ns = _string(st["record_namespace"], "state.record_namespace")
    if not isinstance(st["fields"], dict) or not st["fields"]:
        raise DeclarationError("state.fields: must be a non-empty mapping")
    fields = []
    for name, f in st["fields"].items():
        w = f"state.fields.{name}"
        f = _mapping(f, w, {"type", "default", "nullable"}, {"type", "default"})
        if f["type"] not in PARAM_TYPES:
            raise DeclarationError(f"{w}.type: {f['type']!r} is not one of {list(PARAM_TYPES)}")
        nullable = f.get("nullable", False)
        if not isinstance(nullable, bool):
            raise DeclarationError(f"{w}.nullable: must be a boolean")
        if f["default"] is None:
            if not nullable:
                raise DeclarationError(f"{w}.default: null needs nullable: true")
            default = None
        else:
            default = _coerce(f["default"], f["type"], f"{w}.default")
        fields.append(StateField(name, f["type"], default, nullable))

    outs = []
    if not isinstance(d["outputs"], list) or not d["outputs"]:
        raise DeclarationError("outputs: must be a non-empty list")
    keys = {"kind", "topic", "direction", "producer_id", "source_protocol", "labels"}
    for i, o in enumerate(d["outputs"]):
        w = f"outputs[{i}]"
        o = _mapping(o, w, keys, keys)
        if o["kind"] not in OUTPUT_KINDS:
            raise DeclarationError(
                f"{w}.kind: {o['kind']!r} is not in the kind table {list(OUTPUT_KINDS)}")
        if o["direction"] not in DIRECTIONS:
            raise DeclarationError(
                f"{w}.direction: {o['direction']!r} is not one of {list(DIRECTIONS)}")
        if composition == "terminal" and o["direction"] == "upward":
            raise DeclarationError(
                f"{w}.direction: a terminal unit may not emit upward; "
                "a lossy result is valid only at presentation")
        if o["labels"] != LABELS:
            raise DeclarationError(
                f"{w}.labels: {o['labels']!r} is not {LABELS!r}, the only accepted value")
        outs.append(OutputDecl(
            o["kind"], _string(o["topic"], f"{w}.topic"), o["direction"],
            _string(o["producer_id"], f"{w}.producer_id"),
            _string(o["source_protocol"], f"{w}.source_protocol"), o["labels"],
        ))

    res = _mapping(d["resources"], "resources", {"memory_mi", "cpu_m"}, {"memory_mi", "cpu_m"})
    for k in ("memory_mi", "cpu_m"):
        if not _is_num(res[k]) or res[k] <= 0:
            raise DeclarationError(f"resources.{k}: must be a positive number")

    return Declaration(
        unit=unit, version=version, entrypoint=entrypoint, key="asset_id",
        composition=composition, input_topic=input_topic, roles=tuple(roles),
        params=tuple(params), state_table=table, state_partitions=parts,
        state_record_namespace=ns, state_fields=tuple(fields), outputs=tuple(outs),
        memory_mi=res["memory_mi"], cpu_m=res["cpu_m"],
    )


def load(path: str | Path) -> Declaration:
    with open(path, encoding="utf-8") as fh:
        try:
            raw = yaml.safe_load(fh)
        except yaml.YAMLError as exc:
            raise DeclarationError(f"{path}: not valid YAML: {exc}") from exc
    try:
        return validate(raw)
    except DeclarationError as exc:
        raise DeclarationError(f"{path}: {exc}") from None


def resolve_params(decl: Declaration, overrides: Mapping[str, Any] | None) -> dict[str, int | float]:
    """Declaration defaults with the deployment's overrides applied, typed
    and bounds-checked. An override naming a param the declaration lacks is
    refused (a typo would otherwise run on defaults without a sign)."""
    overrides = dict(overrides or {})
    for k in overrides:
        if decl.param(k) is None:
            raise DeclarationError(f"params override {k!r}: {decl.unit} declares no such param")
    out: dict[str, int | float] = {}
    for p in decl.params:
        v = p.default
        if p.name in overrides:
            w = f"params override {p.name!r}"
            v = _coerce(overrides[p.name], p.type, w)
            if p.minimum is not None and v < p.minimum:
                raise DeclarationError(f"{w}: {v!r} is below minimum {p.minimum!r}")
            if p.maximum is not None and v > p.maximum:
                raise DeclarationError(f"{w}: {v!r} is above maximum {p.maximum!r}")
        out[p.name] = v
    return out


def config_hash(unit: str, version: str, params: Mapping[str, int | float]) -> str:
    """sha256 hex of the canonical JSON of the resolved configuration: the
    same effective parameters hash the same whichever file set them."""
    canon = json.dumps({"unit": unit, "version": version, "params": dict(params)},
                       sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canon.encode("utf-8")).hexdigest()
