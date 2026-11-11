"""Shared loading for the algorithm tests: the real wear declaration and
the wear unit imported from file under the guard, as the runner does."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from algorithm_runtime import declaration, guard

ROOT = Path(__file__).resolve().parents[1]
WEAR_DIR = ROOT / "algorithms" / "wear"


def wear_decl() -> declaration.Declaration:
    return declaration.load(WEAR_DIR / "unit.yaml")


def make_wear(overrides=None):
    """(unit, decl) with params resolved from the declaration defaults plus
    `overrides`."""
    decl = wear_decl()
    guard.check_unit_dir(WEAR_DIR)
    name = "openddil_algorithms.wear"
    spec = importlib.util.spec_from_file_location(name, WEAR_DIR / "wear.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module.Wear(declaration.resolve_params(decl, overrides)), decl


THERMAL_DIR = ROOT / "algorithms" / "thermal"


def thermal_decl() -> declaration.Declaration:
    return declaration.load(THERMAL_DIR / "unit.yaml")


def make_thermal(overrides=None):
    """(unit, decl) for the thermal unit, loaded by file under the guard as
    the runner does, with `overrides` applied to its declared defaults."""
    decl = thermal_decl()
    guard.check_unit_dir(THERMAL_DIR)
    name = "openddil_algorithms.thermal"
    spec = importlib.util.spec_from_file_location(name, THERMAL_DIR / "thermal.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module.ThermalRunaway(declaration.resolve_params(decl, overrides)), decl
