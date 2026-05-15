"""OpenDDIL Phase 5 — Prognostics Derivation Engine (see ADR-0020).

`register(app)` is the one-line seam: faust_edge.py imports it and calls
it once at app load. Everything else is purposely contained:
  - `coefficients.py` — authored placeholder constants (env-overridable)
  - `accumulators.py` — pure per-asset state + observation folding
  - `models.py`        — the four wear models (rules.py mold)
  - `agent.py`         — the ONLY file that imports faust; owns the Table

Phase 5 demonstrates the *mechanism* on synthetic data — see ADR-0020 for
what this engine claims (pipeline works end-to-end) and what it does not
(numbers are accurate). Validation against measured ground truth is the
deferred AFSIM / VR-Forces phase.
"""
from prognostics.agent import register  # noqa: F401

__all__ = ["register"]
