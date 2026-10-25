#!/usr/bin/env python3
"""MonsterMQ Edge build, run, test, release and publish tool - run 'python make.py help'."""

import sys
from pathlib import Path

sys.dont_write_bytecode = True  # no __pycache__ in the tree; a release needs a clean working tree
ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "scripts"))

from mmqlib.cli import run_cli  # noqa: E402
from mmqlib.release import ReleaseConfig, release_commands  # noqa: E402
from tasks import task_commands  # noqa: E402

RELEASE = ReleaseConfig(
    name="MonsterMQ Edge",
    next_steps=[
        "Build artifacts  : python make.py build --all",
        "Publish assets   : python make.py publish --all",
        "Next dev version : python make.py dev",
    ],
)

COMMANDS = {
    **task_commands(ROOT),
    **release_commands(RELEASE, ROOT),
}

if __name__ == "__main__":
    sys.exit(run_cli("make.py", "MonsterMQ Edge", COMMANDS))
