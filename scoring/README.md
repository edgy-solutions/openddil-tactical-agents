# scoring/

Offline harnesses that score algorithm outputs against what happened. Nothing here ships in a runtime
image, and nothing reads or writes a live store.

## rul.py: remaining useful life (ADR-0059 §4.3, proposed)

    python -m scoring.rul --predictions P.jsonl --failures F.jsonl --out REPORT.json

- **The join.** Each remaining-life output, at its meter reading, is joined to the component's next
  confirmed failure on the same meter and unit. The actual remaining use is the difference of the two
  readings. A failure closed with any reason other than `confirmed` (ADR-0059 §4.4) is counted under that
  reason and never joined. A prediction with no confirmed failure after it is censored, not scored.
- **The metrics** are the five Saxena et al. (IJPHM 2010) defines: prognostic horizon, α-λ performance,
  relative accuracy, cumulative relative accuracy and convergence. Each is a named stub that cites the
  paper and raises. They are built from the paper's text when the spec lands; the harness defines none
  of them itself.
- **The field names are provisional.** The remaining-life kind's fields are bound when that kind is
  specified (ADR-0059 §4.2), and the work-order outcome's fields when iagent's architect answers (§4.4).
- **Today's scores would measure a placeholder.** The wear unit's life totals are placeholders (ADR-0059
  finding 7). The harness exists so the first calibrated unit can be scored the day it ships.

Tests: `tests/test_rul_scoring.py`, synthetic failures and predictions only.
