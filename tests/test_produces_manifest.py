"""The code must produce exactly the topics produces.yaml lists.

Offline: the REAL faust-edge and faust-regional apps are built under a fixture
env (no broker). Producible = every Table's changelog topic + every topic object
the code .send()s to + every topic a raw producer sends to. Send sites come from
the AST of the modules the entrypoint loads; each receiver is resolved to the
app.topic(...) call that bound it, and that call's real topic name is read from
the built app. An unresolvable site fails with file:line unless its lines carry
`# produces: <manifest entry>`.

Run with: PYTHONPATH=<contracts>/gen/python py -3 -m pytest tests -q
"""
from __future__ import annotations

import ast
import importlib
import re
import sys
from pathlib import Path

import faust
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import produces_manifest as pm  # noqa: E402

MANIFEST = pm.load(ROOT / "produces.yaml")
SEND = {"send", "send_and_wait"}
ANNOTATION = re.compile(r"#\s*produces:\s*(.+?)\s*$")

EDGE_ENV = {"FAUST_APP_ID": "openddil-edge-tst", "OPENDDIL_EDGE_ID": "edge-tst",
            "OPENDDIL_REGION_ID": "region-tst"}
REGIONAL_ENV = {"OPENDDIL_REGION_ID": "region-tst",
                "REGIONAL_EDGES": "edge-tst=redpanda-edge-tst:9092"}
# Variables the manifests read that the fixture must leave unset so defaults apply.
UNSET = ("REGIONAL_FAN_IN_TOPIC", "REGIONAL_ASSET_REGISTRY_TOPIC", "ELEMENT_TOPIC")


class _Worker:
    """Stands in for faust.Worker only: records the apps main() composes."""
    last = None

    def __init__(self, *services, **_):
        type(self).last = services

    def execute_from_commandline(self):
        return None


def _load_entrypoint(image, monkeypatch):
    """Import the entrypoint under the fixture env.

    Returns (apps, module files, factory kwargs, {(file, line): topic names})."""
    pkg, env = (("edge", EDGE_ENV) if image == "faust-edge" else ("regional", REGIONAL_ENV))
    root = ROOT / pkg
    for k in UNSET:
        monkeypatch.delenv(k, raising=False)
    for k, v in env.items():
        monkeypatch.setenv(k, v)
    monkeypatch.syspath_prepend(str(root))
    before = set(sys.modules)
    topic_names = {}
    orig_topic = faust.App.topic

    def topic(self, *a, **kw):
        t = orig_topic(self, *a, **kw)
        fr = sys._getframe(1)
        topic_names.setdefault((fr.f_code.co_filename, fr.f_lineno), set()).add(t.get_topic_name())
        return t

    monkeypatch.setattr(faust.App, "topic", topic)
    factory_kwargs = {}
    if image == "faust-edge":
        mod = importlib.import_module("faust_edge")
        apps = [mod.app]
    else:
        monkeypatch.setattr(faust, "Worker", _Worker)
        mod = importlib.import_module("faust_regional")

        def wrap(fn):
            def w(**kw):
                factory_kwargs.update(kw)
                return fn(**kw)
            return w

        for name in ("make_aggregator_app", "make_source_app", "make_hq_source_app"):
            monkeypatch.setattr(mod, name, wrap(getattr(mod, name)))
        assert mod.main() == 0
        apps = [s for s in _Worker.last if isinstance(s, faust.App)]
    new = set(sys.modules) - before
    files = {Path(sys.modules[n].__file__).resolve() for n in new
             if getattr(sys.modules[n], "__file__", None)
             and Path(sys.modules[n].__file__).resolve().is_relative_to(root)}
    for n in new:  # leave no entrypoint modules behind for other tests
        monkeypatch.delitem(sys.modules, n, raising=False)
    return apps, files, factory_kwargs, topic_names


def _parents(tree):
    for node in ast.walk(tree):
        for child in ast.iter_child_nodes(node):
            child.parent = node


def _enclosing_fn(node):
    while node is not None and not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        node = getattr(node, "parent", None)
    return node


def _scan(path, topic_names, factory_kwargs, env):
    """Return (resolved topic names, unresolved 'file:line' strings) for one module."""
    src = path.read_text(encoding="utf-8")
    lines = src.splitlines()
    tree = ast.parse(src)
    _parents(tree)
    ours = {fl: n for fl, n in topic_names.items() if Path(fl[0]).resolve() == path}
    # name -> topic names, from every `NAME = app.topic(...)` in this module.
    bound = {}
    for node in ast.walk(tree):
        if (isinstance(node, ast.Assign) and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name) and isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute) and node.value.func.attr == "topic"):
            for (_, ln), names in ours.items():
                if node.value.lineno <= ln <= node.value.end_lineno:
                    bound.setdefault(node.targets[0].id, set()).update(names)

    def resolve(name, fn, seen=()):
        found, aliased = set(), False
        for node in ast.walk(fn) if fn else ():
            if (isinstance(node, ast.Assign) and len(node.targets) == 1
                    and isinstance(node.targets[0], ast.Name) and node.targets[0].id == name
                    and isinstance(node.value, ast.Name) and node.value.id not in seen):
                aliased = True
                got = resolve(node.value.id, fn, seen + (name,))
                if got is None:
                    return None
                found |= got
        return found if aliased else bound.get(name)

    names, bad = set(), []
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in SEND):
            continue
        span = lines[node.lineno - 1:node.end_lineno]
        note = next((m.group(1).strip("\"'") for m in map(ANNOTATION.search, span) if m), None)
        if note:
            names.add(pm.expand(note, env))
            continue
        kw = {k.arg: k.value for k in node.keywords}
        got = None
        if node.args or "topic" in kw:  # raw producer: the topic is an argument
            arg = node.args[0] if node.args else kw["topic"]
            if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                got = {arg.value}
            elif isinstance(arg, ast.Name) and isinstance(factory_kwargs.get(arg.id), str):
                got = {factory_kwargs[arg.id]}
        elif isinstance(node.func.value, ast.Name):  # topic object: resolve its binding
            got = resolve(node.func.value.id, _enclosing_fn(node))
        if got:
            names |= got
        else:
            bad.append(f"{path.relative_to(ROOT)}:{node.lineno}")
    return names, bad


def producible(image, monkeypatch):
    apps, files, factory_kwargs, topic_names = _load_entrypoint(image, monkeypatch)
    env = EDGE_ENV if image == "faust-edge" else REGIONAL_ENV
    names = {t.changelog_topic.get_topic_name() for app in apps for t in app.tables.values()}
    unresolved = []
    for path in sorted(files):
        got, bad = _scan(path, topic_names, factory_kwargs, env)
        names |= got
        unresolved += bad
    return names, unresolved


@pytest.mark.parametrize("image", ["faust-edge", "faust-regional"])
def test_code_produces_exactly_the_manifest(image, monkeypatch):
    env = EDGE_ENV if image == "faust-edge" else REGIONAL_ENV
    declared = {pm.expand(t, env) for t in MANIFEST[image]["topics"]}
    got, unresolved = producible(image, monkeypatch)
    assert not unresolved, ("unresolvable send sites (annotate with '# produces: <entry>'): "
                            + ", ".join(unresolved))
    msg = [f"manifest lists a topic the code does not produce: {t}" for t in sorted(declared - got)]
    msg += [f"code produces a topic the manifest does not list: {t}" for t in sorted(got - declared)]
    assert not msg, f"{image}\n" + "\n".join(msg)


def test_manifest_covers_exactly_both_images():
    assert {w["image"] for w in MANIFEST.values()} == {"openddil/faust-edge", "openddil/faust-regional"}
    assert set(MANIFEST) == {"faust-edge", "faust-regional"}
