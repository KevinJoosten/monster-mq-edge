"""make.py test-integration - build the integration tests to a fixed path and run them.

The Windows firewall remembers "Allow access" per program path. Plain `go test`
builds into a new temp folder on every run, so it would ask every time.
"""

import subprocess
from pathlib import Path
from typing import List

from mmqlib import ui
from mmqlib.cli import Command
from mmqlib.proc import require, run

from .common import EXE, exit_code

HELP = """\
usage: python make.py test-integration [test binary flags]

Builds ./test/integration to bin/integration.test[.exe] and runs it from
test/integration with -test.count=1 -test.timeout=300s plus the given flags.

Example:
  python make.py test-integration -test.run TestPubSub -test.v"""


def main(root: Path, argv: List[str]) -> int:
    if argv and argv[0] in ("-h", "--help", "help"):
        print(HELP)
        return 0
    require("go")
    out = Path("bin") / f"integration.test{EXE}"
    (root / "bin").mkdir(exist_ok=True)
    run(["go", "test", "-c", "-o", out.as_posix(), "./test/integration"], cwd=root)
    cmd = [str(root / out), "-test.count=1", "-test.timeout=300s", *argv]
    ui.detail("$ " + " ".join([out.as_posix(), *cmd[1:]]))
    return exit_code(subprocess.run(cmd, cwd=root / "test" / "integration").returncode)


def commands(root: Path):
    return {"test-integration": Command("build and run the integration tests (test-binary flags pass through)",
                                        lambda argv: main(root, argv))}
