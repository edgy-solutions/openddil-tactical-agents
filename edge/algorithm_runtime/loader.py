"""Import one unit's class from its bundle directory, under the guard.

Shared by the runner and the replay harness, so both load a unit the same
way. No faust import: the replay runs without it.
"""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from algorithm_runtime import guard
from algorithm_runtime.contract import Unit
from algorithm_runtime.declaration import Declaration


def load_unit_class(unit_dir: Path, decl: Declaration, refusal: type[Exception]) -> type:
    """The declared entrypoint class, after `guard.check_unit_dir`. A missing
    file or a class that is not a `contract.Unit` raises `refusal`."""
    guard.check_unit_dir(unit_dir)
    file_name, class_name = decl.entrypoint.split(":")
    path = unit_dir / file_name
    if not path.is_file():
        raise refusal(f"{decl.unit}: entrypoint file {path} does not exist")
    name = f"openddil_algorithms.{decl.unit}"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module  # dataclasses resolves its own module through here
    spec.loader.exec_module(module)
    cls = getattr(module, class_name, None)
    if not (isinstance(cls, type) and issubclass(cls, Unit)):
        raise refusal(f"{decl.unit}: {decl.entrypoint} is not a subclass of contract.Unit")
    return cls
