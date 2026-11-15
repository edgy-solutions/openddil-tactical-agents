"""Seeded, deterministic element envelopes for the element-rollup equivalence:
the element-bands synth with the rollup's extra fields (an `operational` block,
a `condition`, a platform_variant, elements with no id). The recording holds no
element telemetry, so what is compared is the RECORDED TIMING with SYNTHETIC
ELEMENT VALUES; see element_bands_v0/synth.py."""
from element_bands_v0.synth import make_extended as make  # noqa: F401
