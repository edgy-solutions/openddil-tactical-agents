"""The static import/name guard: refusals name file:line, the wear unit passes."""
from __future__ import annotations

import pytest

from algorithm_runtime import guard
from unit_helpers import WEAR_DIR

BAD = [
    ("import socket", "import socket\n"),
    ("import os", "import os\n"),
    ("from urllib", "from urllib import request\n"),
    ("import urllib.request", "import urllib.request\n"),
    ("relative import", "from . import sibling\n"),
    ("runtime module other than contract", "from algorithm_runtime import core\n"),
    ("__import__", "x = __import__('socket')\n"),
    ("open", "f = open('/etc/passwd')\n"),
    ("eval", "eval('1')\n"),
    ("exec", "exec('x=1')\n"),
    ("compile", "compile('1', 'f', 'eval')\n"),
    ("getattr", "getattr(object, 'x', None)\n"),
    ("globals", "globals()\n"),
    ("dunder attribute", "c = ().__class__\n"),
    ("dunder attribute on a chain", "s = ().__class__.__bases__[0].__subclasses__()\n"),
]


@pytest.mark.parametrize("name,src", BAD, ids=[b[0] for b in BAD])
def test_guard_refuses_and_names_the_line(name, src):
    with pytest.raises(guard.GuardError, match=r"unit\.py:2:"):
        guard.check_source("import math\n" + src, "unit.py")


def test_allowlisted_imports_pass():
    guard.check_source(
        "from __future__ import annotations\nimport math\nimport dataclasses\nimport typing\n"
        "from dataclasses import dataclass\nfrom typing import Mapping\n"
        "from algorithm_runtime.contract import Unit, SlidingWindow\n", "ok.py")


def test_the_wear_unit_passes():
    guard.check_unit_dir(WEAR_DIR)


def test_every_py_in_the_unit_dir_is_checked(tmp_path):
    (tmp_path / "unit.py").write_text("import math\n")
    (tmp_path / "helper.py").write_text("import socket\n")
    with pytest.raises(guard.GuardError, match="helper.py:1"):
        guard.check_unit_dir(tmp_path)
