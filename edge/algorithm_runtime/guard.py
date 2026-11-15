"""Static import and name allowlist for a unit's source, run at load.

Every `.py` in the unit directory is parsed (never imported) and refused if
it imports outside the allowlist, names a refused builtin, or touches a
dunder attribute (the way out of any name allowlist is `().__class__`...).
A refusal names file:line.

This is a guard against accident in a reviewed bundle, not a sandbox; see
`contract.py` for what it does and does not promise.
"""
from __future__ import annotations

import ast
from pathlib import Path

ALLOWED_IMPORTS = frozenset({
    "__future__", "math", "dataclasses", "typing", "algorithm_runtime.contract",
})
REFUSED_NAMES = frozenset({
    "__import__", "eval", "exec", "compile", "open", "globals", "locals",
    "vars", "getattr", "setattr", "delattr", "breakpoint", "input",
})


class GuardError(ValueError):
    pass


def check_source(source: str, filename: str) -> None:
    try:
        tree = ast.parse(source, filename=filename)
    except SyntaxError as exc:
        raise GuardError(f"{filename}:{exc.lineno}: does not parse: {exc.msg}") from exc
    for node in ast.walk(tree):
        where = f"{filename}:{getattr(node, 'lineno', 0)}"
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name not in ALLOWED_IMPORTS:
                    raise GuardError(f"{where}: import {alias.name!r} is not on the allowlist")
        elif isinstance(node, ast.ImportFrom):
            if node.level or node.module not in ALLOWED_IMPORTS:
                raise GuardError(
                    f"{where}: import from {'.' * node.level}{node.module or ''} is not on the allowlist")
        elif isinstance(node, ast.Name) and node.id in REFUSED_NAMES:
            raise GuardError(f"{where}: name {node.id!r} is refused")
        elif (isinstance(node, ast.Attribute)
              and node.attr.startswith("__") and node.attr.endswith("__")):
            raise GuardError(f"{where}: attribute {node.attr!r} is refused")


def check_unit_dir(unit_dir: str | Path) -> None:
    files = sorted(Path(unit_dir).rglob("*.py"))
    for path in files:
        check_source(path.read_text(encoding="utf-8"), str(path))
