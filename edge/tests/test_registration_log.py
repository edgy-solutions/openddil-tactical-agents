"""faust-edge prints each algorithm unit's registration line at module load.

Faust configures logging only at worker start, after faust_edge has run its
module code, so without a handler the INFO line is lost. Each case imports
faust_edge in a fresh interpreter and reads stderr.
"""
import os
import re
import subprocess
import sys
from pathlib import Path

EDGE = Path(__file__).resolve().parent.parent
ROOT = EDGE.parent
ALL = ["wear", "thermal", "trends", "posture", "element_bands", "element_rollup"]
LINE = re.compile(r"algorithm unit registered: unit=(\S+) version=(\S*) config_hash=(\S*) ")


def _run(code, enabled):
    env = dict(os.environ)
    env.update({
        "OPENDDIL_EDGE_ID": "edge-test",
        "ALGORITHMS_ENABLED": ",".join(enabled),
        "ALGORITHMS_DIR": str(ROOT / "algorithms"),
        "ALGORITHMS_MEMORY_LIMIT_MI": "1024",
        "ALGORITHMS_CPU_LIMIT_M": "1000",
    })
    proc = subprocess.run([sys.executable, "-c", code], cwd=EDGE, env=env,
                          capture_output=True, text=True, timeout=120)
    assert proc.returncode == 0, proc.stderr[-2000:]
    return proc


def _registered(stderr):
    return [m.groups() for m in map(LINE.search, stderr.splitlines()) if m]


def _check(enabled):
    proc = _run("import faust_edge", enabled)
    lines = _registered(proc.stderr)
    assert sorted(u for u, _, _ in lines) == sorted(enabled), proc.stderr[-2000:]
    for unit, version, config_hash in lines:
        assert version, unit
        assert config_hash and config_hash != "None", unit


def test_every_enabled_unit_prints_one_line():
    _check(ALL)


def test_single_unit():
    _check(["wear"])


def test_handler_is_removed_and_level_restored():
    code = (
        "import logging\n"
        "lg = logging.getLogger('algorithm_runtime')\n"
        "before = lg.level\n"
        "import faust_edge\n"
        "print('STATE', len(lg.handlers), before, lg.level)\n"
    )
    proc = _run(code, ["wear"])
    state = [l for l in proc.stdout.splitlines() if l.startswith("STATE ")]
    assert len(state) == 1, proc.stdout
    _, handlers, before, after = state[0].split()
    assert handlers == "0"
    assert before == after
