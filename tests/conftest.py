"""Import roots for the algorithm tests: the edge image's code root (for
algorithm_runtime) and the frozen oracle. The unit under test is loaded from
algorithms/ by file, the way the runner does it."""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
for p in (ROOT / "edge", ROOT / "tests" / "oracle", ROOT):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))
