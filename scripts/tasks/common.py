"""Paths and build steps shared by the edge make.py commands."""

import argparse
import os
import shutil
import sys
from pathlib import Path
from typing import List, Optional

from mmqlib import ui
from mmqlib.proc import CommandError, require, run
from mmqlib.version import read_version_file, try_parse

IS_WINDOWS = os.name == "nt"
EXE = ".exe" if IS_WINDOWS else ""
GO_PKG = "./cmd/monstermq-edge"
VERSION_LDFLAG = "-X monstermq.io/edge/internal/version.Version="


def read_version(root: Path) -> str:
    """First line of version.txt, as the old scripts read it."""
    return read_version_file(root / "version.txt")


def is_dev_version(version: str) -> bool:
    """Pre-release versions (X.Y.Z-dev, -beta, -alpha, ...) never get :latest or a GitHub release."""
    parsed = try_parse(version)
    return parsed.is_prerelease if parsed else version.endswith(("-dev", "-beta", "-alpha"))


def binary_path(root: Path) -> Path:
    return root / "bin" / f"monstermq-edge{EXE}"


def parser(prog: str, description: str, epilog: Optional[str] = None) -> argparse.ArgumentParser:
    return argparse.ArgumentParser(prog=f"make.py {prog}", description=description, epilog=epilog,
                                   formatter_class=argparse.RawDescriptionHelpFormatter)


# ---------------------------------------------------------------------------
# Dashboard (embedded into the binary from internal/dashboard/dist)
# ---------------------------------------------------------------------------

def _dashboard_source(root: Path) -> Path:
    link = root / "dashboard"
    if (link.exists() or link.is_symlink()) and not link.is_dir():
        ui.warn("'dashboard' link is broken; removing it")
        link.unlink()
    if link.is_dir():
        return link
    sibling = root.parent / "dashboard"
    if not sibling.is_dir():
        raise CommandError(["dashboard"], 1,
                           "Dashboard not found: neither ./dashboard nor ../dashboard exists.\n"
                           "Clone the dashboard repository next to edge, or link it: ln -s ../dashboard dashboard")
    try:
        os.symlink(Path("..") / "dashboard", link, target_is_directory=True)
        ui.info("Created symlink 'dashboard' -> ../dashboard")
        return link
    except OSError:
        # Windows without developer mode cannot create symlinks; the sibling works just as well.
        ui.info("Using ../dashboard (could not create a 'dashboard' symlink here)")
        return sibling


def _newest_mtime(paths) -> float:
    newest = 0.0
    for p in paths:
        if p.is_file():
            newest = max(newest, p.stat().st_mtime)
        elif p.is_dir():
            for f in p.rglob("*"):
                if f.is_file():
                    newest = max(newest, f.stat().st_mtime)
    return newest


def dashboard_build_available(src: Path) -> bool:
    """True if the dashboard checkout already has a build in dist/ (warns when it looks stale)."""
    index = src / "dist" / "index.html"
    if not index.is_file():
        return False
    # The newest file in dist/ is the build time; some files keep their source timestamps
    # when Vite copies them, so index.html alone is not reliable.
    if _newest_mtime([src / "src", src / "package.json", src / "vite.config.js"]) > _newest_mtime([src / "dist"]):
        ui.warn(f"dashboard sources changed after {src / 'dist'} was built; to include them, "
                f"rebuild it in {src} with 'python make.py build --web'")
    return True


def prepare_dashboard(root: Path) -> None:
    """Same as the Makefile's prepare-dashboard: use the dashboard checkout's existing build,
    build it only if there is none, and sync its dist into internal/dashboard/dist.

    Dependencies are only installed (npm ci) when node_modules is missing, e.g. a fresh
    checkout in CI. An existing node_modules is never reinstalled: npm ci deletes it first,
    which fails on Windows while a dashboard dev server holds esbuild.exe open, and npm install
    may rewrite the dashboard's package-lock.json.
    """
    src = _dashboard_source(root)
    dist = src / "dist"
    if dashboard_build_available(src):
        ui.info(f"Using existing dashboard build from {dist}")
    else:
        ui.step(f"No dashboard build found in {src}. Building dashboard...")
        require("npm")
        if not (src / "node_modules").is_dir():
            run(["npm", "ci"], cwd=src)
        run(["npm", "run", "build"], cwd=src)
    target = root / "internal" / "dashboard" / "dist"
    ui.info(f"Syncing dashboard from {dist} ...")
    target.mkdir(parents=True, exist_ok=True)
    shutil.copytree(dist, target, dirs_exist_ok=True)


# ---------------------------------------------------------------------------
# Go builds
# ---------------------------------------------------------------------------

def build_binary(root: Path) -> Path:
    """Native, static (CGO_ENABLED=0) broker binary with the version injected."""
    require("go")
    prepare_dashboard(root)
    version = read_version(root)
    out = binary_path(root)
    out.parent.mkdir(parents=True, exist_ok=True)
    run(["go", "build", "-trimpath", "-ldflags", f"-s -w {VERSION_LDFLAG}{version}",
         "-o", out.relative_to(root).as_posix(), GO_PKG], cwd=root, env={"CGO_ENABLED": "0"})
    return out


def find_bash() -> Optional[str]:
    """bash for running scripts/*.sh. On Windows prefer Git Bash over WSL's System32 bash."""
    if IS_WINDOWS:
        for base in (os.environ.get("ProgramFiles"), os.environ.get("ProgramW6432")):
            if base:
                git_bash = Path(base) / "Git" / "bin" / "bash.exe"
                if git_bash.is_file():
                    return str(git_bash)
    return shutil.which("bash")


def clean_outputs(root: Path) -> List[Path]:
    """Remove build outputs (Makefile 'clean' plus dist/ and the saved Docker tar). Returns what was removed."""
    removed = []
    for rel in ("bin", "dist", "build/embed", "build/embed-check", "build/embed-asan"):
        path = root / rel
        if path.exists():
            shutil.rmtree(path)
            removed.append(path)
    dashboard_dist = root / "internal" / "dashboard" / "dist"
    if dashboard_dist.is_dir():
        for entry in dashboard_dist.iterdir():
            if entry.name == "placeholder.html":
                continue
            if entry.is_dir() and not entry.is_symlink():
                shutil.rmtree(entry)
            else:
                entry.unlink()
            removed.append(entry)
    tar = root / "docker" / "monstermq-edge.tar"
    if tar.exists():
        tar.unlink()
        removed.append(tar)
    return removed


def exit_code(rc: int) -> int:
    """Pass a child's exit code through; a POSIX signal (-N) becomes 128+N like in a shell."""
    return 128 - rc if rc < 0 else rc


def interactive() -> bool:
    return sys.stdin.isatty()
