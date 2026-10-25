"""make.py winccoa - build the WinCC OA API manager WCCOAmmq (broker embedded as c-archive).

[1/2] libmonstermq.a: Go c-archive (CGO, -tags winccoa_embed) into build/embed, like the
      Makefile's embed-lib. On Windows cgo needs MinGW gcc (found on PATH or in WinGet packages).
[2/2] WCCOAmmq: CMake project in winccoa/manager (a separate repository checked out at winccoa/).
      Linux: default generator. Windows: Visual Studio generator, x64, --config.
"""

import os
import re
import shutil
import subprocess
import time
from pathlib import Path
from typing import List, Optional

from mmqlib import ui
from mmqlib.cli import Command
from mmqlib.proc import CommandError, capture, require, run, which

from .common import IS_WINDOWS, VERSION_LDFLAG, parser, prepare_dashboard, read_version

DEFAULT_API_ROOT = (r"C:\Program Files\Siemens\WinCC_OA\3.21\api" if IS_WINDOWS
                    else "/opt/WinCC_OA/3.21/api")
VS_GENERATORS = {"18": "Visual Studio 18 2026", "17": "Visual Studio 17 2022",
                 "16": "Visual Studio 16 2019", "15": "Visual Studio 15 2017"}


# ---------------------------------------------------------------------------
# Toolchain lookup (Windows specifics from the old build.ps1)
# ---------------------------------------------------------------------------

def _refresh_windows_path() -> None:
    """Add machine + user PATH from the registry, so tools installed after this shell started are found."""
    import winreg
    parts = []
    for hive, key in ((winreg.HKEY_LOCAL_MACHINE, r"SYSTEM\CurrentControlSet\Control\Session Manager\Environment"),
                      (winreg.HKEY_CURRENT_USER, "Environment")):
        try:
            with winreg.OpenKey(hive, key) as k:
                parts.append(os.path.expandvars(winreg.QueryValueEx(k, "Path")[0]))
        except OSError:
            pass
    os.environ["PATH"] = os.pathsep.join([*parts, os.environ.get("PATH", "")])


def _ensure_gcc() -> str:
    if which("gcc"):
        return "gcc"
    if IS_WINDOWS:
        packages = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WinGet" / "Packages"
        if packages.is_dir():
            found = next(packages.rglob("gcc.exe"), None)
            if found:
                os.environ["PATH"] = str(found.parent) + os.pathsep + os.environ["PATH"]
                return "gcc"
        raise CommandError(["gcc"], 127, "gcc not found on PATH. Go's cgo needs a GCC (e.g. WinLibs MinGW):\n"
                           "  winget install BrechtSanders.WinLibs.POSIX.UCRT --scope user")
    cc = os.environ.get("CC") or ("cc" if which("cc") else None)
    if not cc:
        raise CommandError(["cc"], 127, "no C compiler found (set CC or install gcc)")
    return cc


def _visual_studio():
    """(installationPath, installationVersion) of the latest Visual Studio, or ('', '')."""
    vswhere = Path(os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")) / \
        "Microsoft Visual Studio" / "Installer" / "vswhere.exe"
    if not vswhere.is_file():
        return "", ""
    path = capture([vswhere, "-latest", "-property", "installationPath"], check=False)
    version = capture([vswhere, "-latest", "-property", "installationVersion"], check=False)
    return path, version


def _windows_cmake_and_generator(generator: Optional[str]):
    vs_path, vs_version = _visual_studio()
    cmake = None
    if vs_path:
        bundled = Path(vs_path) / "Common7" / "IDE" / "CommonExtensions" / "Microsoft" / "CMake" / "CMake" / "bin" / "cmake.exe"
        if bundled.is_file():
            cmake = str(bundled)  # preferred over e.g. the MinGW cmake, which cannot drive MSVC well
    if cmake is None:
        cmake = which("cmake")
    if cmake is None:
        raise CommandError(["cmake"], 127, "cmake not found. Install CMake or the Visual Studio C++ CMake tools.")
    if not generator:
        generator = VS_GENERATORS.get(vs_version.split(".")[0] if vs_version else "")
        if not generator:
            generator = "Visual Studio 17 2022" if "2022" in vs_path else "Visual Studio 18 2026"
    return cmake, generator


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

def build_embed_lib(root: Path, cc: str, run_test: bool) -> None:
    """Makefile embed-lib (+ embed-test with run_test)."""
    prepare_dashboard(root)
    out = root / "build" / "embed"
    out.mkdir(parents=True, exist_ok=True)
    version = read_version(root)
    run(["go", "build", "-trimpath", "-tags", "winccoa_embed", "-buildmode=c-archive",
         "-ldflags", f"{VERSION_LDFLAG}{version}", "-o", "build/embed/libmonstermq.a", "./embed/cabi"],
        cwd=root, env={"CGO_ENABLED": "1", "CC": cc})
    for header in ("monstermq.h", "monstermq_types.h"):
        shutil.copy2(root / "embed" / "cabi" / header, out / header)
    if run_test:
        ui.step("Running the C ABI harness...")
        harness = out / "abi_harness"
        run([cc, "-std=c11", "-Wall", "-Wextra", "-Werror", "-Ibuild/embed", "-o", harness.relative_to(root).as_posix(),
             "embed/harness/abi_harness.c", "build/embed/libmonstermq.a", "-lpthread"], cwd=root)
        run([str(harness)], cwd=root / "embed" / "harness")


def _move_aside_locked(path: Path) -> None:
    """Windows locks a running exe against overwriting but allows renaming it."""
    stamp = int(time.time() * 1000)
    for old in path.parent.glob(f"{path.stem}.*.old"):
        try:
            old.unlink()
        except OSError:
            pass
    try:
        path.unlink()
    except OSError:
        path.replace(path.with_name(f"{path.stem}.{stamp}.old"))
        pdb = path.with_suffix(".pdb")
        if pdb.exists():
            try:
                pdb.replace(path.with_name(f"{pdb.name}.{stamp}.old"))
            except OSError:
                pass


def build_manager(manager_dir: Path, build_dir: Path, clean: bool, cmake: str,
                  generator: Optional[str], config: str) -> Path:
    if clean and build_dir.exists():
        ui.warn(f"Cleaning CMake build directory: {build_dir}")
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True, exist_ok=True)
    if IS_WINDOWS:
        run([cmake, "-B", build_dir, "-S", manager_dir, "-G", generator, "-A", "x64"])
        existing = build_dir / config / "WCCOAmmq.exe"
        if existing.exists():
            _move_aside_locked(existing)
        run([cmake, "--build", build_dir, "--config", config])
        for candidate in (build_dir / config / "WCCOAmmq.exe", build_dir / "WCCOAmmq.exe"):
            if candidate.is_file():
                return candidate
        raise CommandError(["cmake"], 1, f"built binary WCCOAmmq.exe not found in {build_dir}")
    capture([cmake, "-S", manager_dir, "-B", build_dir])  # configure output stays quiet, as before
    run([cmake, "--build", build_dir, "--parallel", str(os.cpu_count() or 1)])
    return build_dir / "WCCOAmmq"


def install(binary: Path, project: Path) -> None:
    bin_dir = project / "bin"
    target = bin_dir / binary.name
    tmp = bin_dir / f".{binary.name}.new"
    # Copy to a temp name and rename, so a running manager keeps its binary.
    shutil.copy2(binary, tmp)
    try:
        os.replace(tmp, target)
    except PermissionError:
        _move_aside_locked(target)  # Windows: target still in use
        os.replace(tmp, target)
    pdb = binary.with_suffix(".pdb")
    if IS_WINDOWS and pdb.is_file():
        try:
            shutil.copy2(pdb, bin_dir / pdb.name)
        except OSError as e:
            ui.warn(f"could not copy {pdb.name}: {e}")
    ui.ok(f"Installed to: {target}")


def _manager_running(index: str) -> bool:
    if IS_WINDOWS:  # tasklist cannot see the command line: any WCCOAmmq counts, as in build.ps1
        out = capture(["tasklist", "/FO", "CSV", "/NH", "/FI", "IMAGENAME eq WCCOAmmq.exe"], check=False)
        return '"WCCOAmmq.exe"' in out
    return subprocess.run(["pgrep", "-f", f"WCCOAmmq .*-pmonIndex {index}( |$)"],
                          stdout=subprocess.DEVNULL).returncode == 0


def stop_manager(index: str) -> None:
    ui.step(f"Stopping PMON manager {index}...")
    run(["woa", "pmon", "manager", "stop", index], check=False)
    for _ in range(30):
        if not _manager_running(index):
            return
        time.sleep(1)
    if IS_WINDOWS:
        ui.warn("a WCCOAmmq process is still running after 30s (maybe another index); continuing")
        return
    raise CommandError(["woa"], 1, f"manager {index} did not stop within 30s")


def start_manager(index: str) -> None:
    ui.step(f"Starting PMON manager {index}...")
    run(["woa", "pmon", "manager", "start", index])
    ui.ok(f"Manager {index} started")


# ---------------------------------------------------------------------------

def main(root: Path, argv: List[str]) -> int:
    p = parser("winccoa", "Build the WinCC OA API manager WCCOAmmq (broker embedded as c-archive).",
               epilog="Environment: API_ROOT (WinCC OA API path), CC (POSIX), WOA_PMON_* for --restart.")
    p.add_argument("--test", action="store_true", help="also build and run the C ABI harness (embed-test)")
    p.add_argument("--clean", action="store_true", help="remove the CMake build directory first")
    p.add_argument("--install", metavar="PROJECT", help="copy WCCOAmmq to PROJECT/bin")
    p.add_argument("--restart", metavar="INDEX", help="stop and start PMON manager INDEX (needs woa)")
    p.add_argument("--api-root", default=os.environ.get("API_ROOT") or DEFAULT_API_ROOT,
                   help=f"WinCC OA API path (default: $API_ROOT or {DEFAULT_API_ROOT})")
    p.add_argument("--config", default="RelWithDebInfo", help="Windows: CMake build configuration (default: %(default)s)")
    p.add_argument("--generator", help="Windows: CMake Visual Studio generator override")
    args = p.parse_args(argv)

    if args.test and IS_WINDOWS:
        p.error("--test runs the C ABI harness (embed/harness), which is POSIX-only (arpa/inet.h, "
                "pthreads); run it on Linux or macOS")
    if args.restart is not None and not re.fullmatch(r"\d+", args.restart):
        p.error("--restart needs a numeric PMON manager index")
    project = Path(args.install) if args.install else None
    if project is not None and not (project / "bin").is_dir():
        ui.error(f"{project / 'bin'} not found")
        return 1

    manager_dir = root / "winccoa" / "manager"
    build_dir = manager_dir / "build"
    if not (manager_dir / "CMakeLists.txt").is_file():
        ui.error(f"{manager_dir} not found. winccoa/ is a separate repository; clone "
                 "github.com/vogler75/monster-mq-edge-winccoa into winccoa/")
        return 1

    api_root = Path(args.api_root)
    if not (api_root / "CMakeDefines.txt").is_file():
        ui.error(f"WinCC OA API not found at {api_root} (CMakeDefines.txt missing). Use --api-root or set API_ROOT.")
        return 1
    # CMakeLists.txt reads $ENV{API_ROOT}; CMake wants forward slashes.
    os.environ["API_ROOT"] = api_root.as_posix() if IS_WINDOWS else str(api_root)

    if IS_WINDOWS:
        _refresh_windows_path()
    require("go")
    cc = _ensure_gcc()
    if IS_WINDOWS:
        cmake, generator = _windows_cmake_and_generator(args.generator)
    else:
        cmake, generator = require("cmake"), None
    if args.restart is not None and which("woa") is None:
        ui.error(f"woa not found; cannot restart manager {args.restart} (restart it in the WinCC OA console)")
        return 1

    ui.header(f"WCCOAmmq Build (API_ROOT={api_root})")
    ui.step("[1/2] Building libmonstermq.a...")
    build_embed_lib(root, cc, args.test)

    ui.step(f"[2/2] Building WCCOAmmq{f' ({generator}, {args.config})' if IS_WINDOWS else ''}...")
    binary = build_manager(manager_dir, build_dir, args.clean, cmake, generator, args.config)
    ui.ok(f"Manager built at: {binary}")

    if IS_WINDOWS:
        # A running exe cannot be replaced in place: stop first, install, then start.
        if args.restart is not None:
            stop_manager(args.restart)
        if project is not None:
            install(binary, project)
        if args.restart is not None:
            start_manager(args.restart)
    else:
        if project is not None:
            install(binary, project)
        if args.restart is not None:
            stop_manager(args.restart)
            start_manager(args.restart)

    ui.header("Build Complete")
    return 0


def commands(root: Path):
    return {"winccoa": Command("build the WinCC OA manager WCCOAmmq (--test, --clean, --install, --restart)",
                               lambda argv: main(root, argv))}
