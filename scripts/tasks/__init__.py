"""MonsterMQ Edge make.py commands (repo-specific; generic helpers live in mmqlib)."""

from pathlib import Path
from typing import Dict

from mmqlib.cli import Command


def task_commands(root: Path) -> Dict[str, Command]:
    from . import build, image, integration, publish, run, winccoa
    commands: Dict[str, Command] = {}
    for module in (build, image, publish, run, integration, winccoa):
        commands.update(module.commands(root))
    return commands
