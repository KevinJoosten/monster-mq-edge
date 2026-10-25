"""Running external commands the same way on Windows, macOS and Linux."""

import os
import shutil
import subprocess
from pathlib import Path
from typing import Dict, List, Mapping, Optional, Sequence, Union

from . import ui

PathLike = Union[str, Path]


class CommandError(Exception):
    def __init__(self, cmd: Sequence[str], returncode: int, output: str = ""):
        self.cmd = list(cmd)
        self.returncode = returncode
        self.output = output.strip()
        msg = f"command failed (exit {returncode}): {' '.join(self.cmd)}"
        if self.output:
            msg += f"\n{self.output}"
        super().__init__(msg)


def which(name: str) -> Optional[str]:
    """Resolve a tool on PATH, including Windows wrappers like npm.cmd or mvn.cmd."""
    return shutil.which(name)


def require(name: str) -> str:
    path = which(name)
    if path is None:
        raise CommandError([name], 127, f"'{name}' not found on PATH")
    return path


def _env(extra: Optional[Mapping[str, str]]) -> Optional[Dict[str, str]]:
    """The current environment plus `extra` (None when there is nothing to add)."""
    if not extra:
        return None
    env = dict(os.environ)
    env.update({k: str(v) for k, v in extra.items()})
    return env


def _resolve(cmd: Sequence[PathLike]) -> List[str]:
    args = [str(c) for c in cmd]
    exe = which(args[0])
    if exe:
        args[0] = exe
    return args


def run(cmd: Sequence[PathLike], cwd: Optional[PathLike] = None, echo: bool = True,
        check: bool = True, env: Optional[Mapping[str, str]] = None) -> int:
    """Run a command with output streaming to the console. Never uses a shell.

    `env` holds variables to set on top of the current environment.
    """
    args = _resolve(cmd)
    if echo:
        prefix = " ".join(f"{k}={v}" for k, v in (env or {}).items())
        ui.detail("$ " + (prefix + " " if prefix else "") + " ".join(str(c) for c in cmd))
    proc = subprocess.run(args, cwd=cwd, env=_env(env))
    if check and proc.returncode != 0:
        raise CommandError(args, proc.returncode)
    return proc.returncode


def capture(cmd: Sequence[PathLike], cwd: Optional[PathLike] = None, check: bool = True,
            env: Optional[Mapping[str, str]] = None) -> str:
    """Run a command and return its stdout (stripped)."""
    args = _resolve(cmd)
    proc = subprocess.run(args, cwd=cwd, capture_output=True, text=True,
                          encoding="utf-8", errors="replace", env=_env(env))
    if check and proc.returncode != 0:
        raise CommandError(args, proc.returncode, proc.stderr or proc.stdout)
    return proc.stdout.strip()


def succeeds(cmd: Sequence[PathLike], cwd: Optional[PathLike] = None) -> bool:
    """True when the command exists and exits 0 (its output is discarded)."""
    args = [str(c) for c in cmd]
    exe = which(args[0])
    if exe is None and not Path(args[0]).is_file():
        return False
    if exe:
        args[0] = exe
    try:
        proc = subprocess.run(args, cwd=cwd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except OSError:
        return False
    return proc.returncode == 0
