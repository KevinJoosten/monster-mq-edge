"""make.py run - (build and) start the broker."""

import os
import subprocess
import time
from pathlib import Path
from typing import List

from mmqlib import ui
from mmqlib.cli import Command
from mmqlib.proc import capture, require, run, which

from .common import IS_WINDOWS, binary_path, build_binary, exit_code

HELP = """\
MonsterMQ Edge run script (Go broker).
usage: python make.py run [SCRIPT_OPTIONS] [-- BROKER_OPTIONS]

Script options (before --):
  -b,  -build       Build the binary (CGO_ENABLED=0) before starting
  -c,  -compile     Type-check / compile all packages (no binary output)
  -n,  -norun       Don't start the broker (combine with -b/-c for build-only)
  -nk, -nokill      Don't kill any already-running monstermq-edge first
  -h,  -help        Show this help
  (each option also works with two dashes, e.g. --build)

Broker options (after --):
  Anything after -- is passed straight to the binary. Unknown options before --
  are passed on as well. See bin/monstermq-edge -h (-config, -log-level, -version).

Examples:
  python make.py run                       Start with config.yaml (or config.yaml.example)
  python make.py run -b                    Build, then start
  python make.py run -b -n                 Build only
  python make.py run -b -- -config foo.yaml"""

FLAGS = {
    "b": "build", "build": "build",
    "c": "compile", "compile": "compile",
    "n": "norun", "norun": "norun",
    "nk": "nokill", "nokill": "nokill",
    "h": "help", "help": "help", "?": "help",
}


def parse(argv: List[str]):
    opts = {"build": False, "compile": False, "norun": False, "nokill": False, "help": False}
    broker: List[str] = []
    it = iter(argv)
    for arg in it:
        if arg == "--":
            broker.extend(it)
            break
        key = FLAGS.get(arg.lstrip("-")) if arg.startswith("-") else None
        if key:
            opts[key] = True
        else:
            broker.append(arg)
    return opts, broker


def _has_config(args: List[str]) -> bool:
    return any(a in ("-config", "--config") or a.startswith(("-config=", "--config=")) for a in args)


def _running_pids(binary: Path) -> List[str]:
    if IS_WINDOWS:
        out = capture(["tasklist", "/FO", "CSV", "/NH", "/FI", f"IMAGENAME eq {binary.name}"], check=False)
        return [line.split('","')[1] for line in out.splitlines() if line.startswith(f'"{binary.name}"')]
    if which("pgrep") is None:
        ui.warn("pgrep not found; not checking for running instances")
        return []
    out = capture(["pgrep", "-f", "bin/monstermq-edge"], check=False)
    return [pid for pid in out.split() if pid.isdigit() and int(pid) != os.getpid()]


def kill_existing(binary: Path) -> None:
    pids = _running_pids(binary)
    if not pids:
        return
    ui.info(f"Killing existing instances: {' '.join(pids)}")
    if IS_WINDOWS:
        # Console programs do not react to a polite taskkill; the old run.ps1 forced it too.
        capture(["taskkill", "/F", "/IM", binary.name], check=False)
    else:
        capture(["kill", *pids], check=False)
    for _ in range(5):
        time.sleep(0.5)
        if not _running_pids(binary):
            return
    still = _running_pids(binary)
    if still and not IS_WINDOWS:
        capture(["kill", "-9", *still], check=False)


def main(root: Path, argv: List[str]) -> int:
    opts, broker = parse(argv)
    if opts["help"]:
        print(HELP)
        return 0
    binary = binary_path(root)

    if opts["build"]:
        ui.step("Building monstermq-edge...")
        out = build_binary(root)
        ui.ok(f"Built: {out.relative_to(root).as_posix()}")
    if opts["compile"] and not opts["build"]:
        ui.step("Compiling all packages...")
        require("go")
        run(["go", "build", "./..."], cwd=root)
    if opts["norun"]:
        return 0

    if not binary.is_file():
        ui.error(f"Binary {binary.relative_to(root).as_posix()} not found. Run with -b first.")
        return 1
    if not opts["nokill"]:
        kill_existing(binary)

    # Pick a config: explicit -config wins; otherwise prefer config.yaml, then the example.
    if not _has_config(broker):
        for name in ("config.yaml", "config.yaml.example"):
            if (root / name).is_file():
                broker = ["-config", name, *broker]
                break

    ui.info(f"Starting: {binary.relative_to(root).as_posix()} {' '.join(broker)}")
    proc = subprocess.Popen([str(binary), *broker], cwd=root)
    interrupts = 0
    while True:
        try:
            return exit_code(proc.wait())
        except KeyboardInterrupt:
            # Ctrl+C reaches the broker too: wait for its clean shutdown; a second Ctrl+C kills it.
            interrupts += 1
            if interrupts > 1:
                proc.kill()


def commands(root: Path):
    return {"run": Command("(build and) start the broker; options before --, broker flags after",
                           lambda argv: main(root, argv))}
