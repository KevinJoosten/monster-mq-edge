"""make.py build / clean - native binary, Debian packages and local Docker image."""

from pathlib import Path
from typing import List

from mmqlib import docker, ui
from mmqlib.cli import Command
from mmqlib.proc import CommandError, require, run, succeeds

from . import image
from .common import IS_WINDOWS, build_binary, clean_outputs, find_bash, parser, read_version

DEB_ARCHES = ("arm64", "armhf", "amd64")  # same order as the Makefile's deb-all

DEB_ON_WINDOWS = (
    "Debian packages are built by scripts/build-deb.sh, which needs bash, GNU tar and 'ar' (binutils).\n"
    "On Windows: install Git for Windows (Git Bash) and put a MinGW 'ar' on PATH, e.g.\n"
    "  winget install BrechtSanders.WinLibs.POSIX.UCRT --scope user\n"
    "or run 'python3 make.py build --deb' in WSL / on Linux."
)


def _deb_bash() -> str:
    """bash that can run scripts/build-deb.sh, or raise with what is missing."""
    bash = find_bash()
    if bash is None:
        raise CommandError(["bash"], 127, "--deb needs bash to run scripts/build-deb.sh, but none was found.\n"
                           + (DEB_ON_WINDOWS if IS_WINDOWS else ""))
    if not succeeds([bash, "-c", "command -v ar >/dev/null && command -v tar >/dev/null"]):
        raise CommandError(["ar"], 127, "'ar' (binutils) or 'tar' not found in the bash environment.\n"
                           + (DEB_ON_WINDOWS if IS_WINDOWS else "Install binutils (e.g. apt install binutils)."))
    return bash


def build_debs(root: Path, bash: str) -> None:
    for arch in DEB_ARCHES:
        ui.info(f"  -> {arch}")
        run([bash, "scripts/build-deb.sh", "--arch", arch], cwd=root)


def main(root: Path, argv: List[str]) -> int:
    p = parser("build", "Build MonsterMQ Edge artifacts.",
               epilog="Outputs: bin/monstermq-edge[.exe], bin/monstermq-edge_<version>_<arch>.deb,\n"
                      "Docker image rocworks/monstermq-edge:<version> (+ docker/monstermq-edge.tar).")
    p.add_argument("--all", action="store_true", help="build all artifacts (binary, deb, docker)")
    p.add_argument("--binary", action="store_true", help="build native Go binary for the current machine")
    p.add_argument("--deb", "-deb", action="store_true", help="build Debian packages (arm64, armhf, amd64)")
    p.add_argument("--docker", action="store_true", help="build local Docker image (native platform)")
    p.add_argument("--clean", action="store_true", help="clean build output directories first")
    if not argv:
        p.print_help()
        return 0
    args = p.parse_args(argv)
    if args.all:
        args.binary = args.deb = args.docker = True

    version = read_version(root)

    # Check every selected target's tools before building anything.
    bash = None
    if args.binary or args.deb:
        require("go")
    if args.deb:
        bash = _deb_bash()
    if args.docker and not docker.daemon_running():
        raise CommandError(["docker"], 1, "docker is not on PATH or the Docker daemon is not running")

    ui.header(f"MonsterMQ Edge Build Pipeline (v{version})")

    if args.clean:
        ui.step("Cleaning output build directories...")
        for path in clean_outputs(root):
            ui.detail(f"removed {path.relative_to(root).as_posix()}")
        ui.ok("Clean complete")

    if args.binary:
        ui.step("[1/3] Building native Go binary...")
        out = build_binary(root)
        ui.ok(f"Native binary built at: {out.relative_to(root).as_posix()}")

    if args.deb:
        ui.step("[2/3] Building Debian packages (arm64, armhf, amd64)...")
        build_debs(root, bash)
        ui.ok("Debian packages built in: bin/")

    if args.docker:
        ui.step("[3/3] Building local Docker image...")
        image.build_image(root, testing=False, push=False, save_tar=True)
        ui.ok("Local Docker image built")

    ui.header("Build Complete")
    return 0


def commands(root: Path):
    return {
        "build": Command("build artifacts: --all, --binary, --deb, --docker, --clean",
                         lambda argv: main(root, argv)),
        "clean": Command("remove build outputs (same as 'build --clean')",
                         lambda argv: main(root, ["--clean", *argv])),
    }
