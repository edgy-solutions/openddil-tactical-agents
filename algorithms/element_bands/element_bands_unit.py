"""Schmitt-trigger health bands for element fields, as a declared unit.

Why a Schmitt trigger: a health value that dithers around a limit would
otherwise flip bands on every sample and flood the event topic. The band rises
to k when the value passes limits[k-1], but falls below band k only when the
value drops under limits[k-1] - hysteresis, so noise inside that margin is not
a crossing.

The first observation of an (asset, element, field) sets its band and is not a
crossing: a restart must not announce every element as having "entered" its
band. Non-numeric values and unconfigured fields are ignored; nothing is
defaulted. The logic is the old `BandTracker.update`, operation for operation;
the tracker's dict is this unit's `bands_by_key` state.

The unit states what crossed. Severity, the event time and the labels are the
host's (declared bands, the envelope's observed_at_ns, the envelope).

Stdlib only.
"""
from __future__ import annotations

from algorithm_runtime.contract import Unit


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


class ElementBands(Unit):
    def step(self, sample, state):
        bands = self.params["bands"]
        held = state["bands_by_key"]
        crossings = []
        for element_id, layer_name, element in sample["elements"]:
            for field, spec in bands.items():
                value = element.get(field)
                if not _is_num(value):
                    continue
                limits, names, h = spec["limits"], spec["names"], spec["hysteresis"]
                per_element = held.setdefault(element_id, {})
                old = per_element.get(field)

                # Highest band whose limit the value is strictly above.
                up = sum(1 for lim in limits if value > lim)
                if old is None:
                    per_element[field] = up
                    continue
                new = old
                if up > old:
                    new = up
                else:
                    # Fall only while below the band's limit minus the margin.
                    while new > 0 and value < limits[new - 1] - h:
                        new -= 1
                if new == old:
                    continue
                per_element[field] = new
                crossings.append({
                    "element_id": element_id, "layer_name": layer_name, "field": field,
                    "from": names[old], "to": names[new], "value": value,
                })
        return {"band_crossing": crossings}
