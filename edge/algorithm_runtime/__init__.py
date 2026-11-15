"""Declared analytic units on the edge Faust app (ADR-0034, first slice).

A unit is a directory holding `unit.yaml` (its declaration) and the Python
that implements `contract.Unit`. `runner.register(app)` loads the enabled
units and wires each to its input topic and its durable state table.

Only `runner.py` imports faust; every other module here is importable and
testable without it.
"""
