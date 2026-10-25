"""make.py docker - build the Docker image locally, or multi-arch and push to Docker Hub."""

import os
from pathlib import Path
from typing import List

from mmqlib import docker, ui
from mmqlib.cli import Command
from mmqlib.proc import CommandError, run, succeeds
from mmqlib.version import VersionError

from .common import interactive, is_dev_version, parser, prepare_dashboard, read_version

TAR_NAME = "monstermq-edge.tar"
BUILDER_NAME = "monstermq-edge-builder"


def image_repo() -> str:
    return os.environ.get("IMAGE_REPO", "rocworks/monstermq-edge")


def platforms() -> str:
    return os.environ.get("PLATFORMS", "linux/amd64,linux/arm64,linux/arm/v7")


def image_tags(root: Path, testing: bool):
    """(build-arg VERSION, [tags]) the way docker/build.sh chose them."""
    repo = image_repo()
    if testing:
        return "testing", [f"{repo}:testing"]
    try:
        full = read_version(root)
    except VersionError:  # no version.txt: the old script fell back to 'testing'
        return "testing", [f"{repo}:testing"]
    version = full.split("+", 1)[0]
    tags = [f"{repo}:{version}"]
    if not is_dev_version(version):
        tags.append(f"{repo}:latest")
    return full, tags


def _tag_args(tags: List[str]) -> List[str]:
    args: List[str] = []
    for tag in tags:
        args += ["-t", tag]
    return args


def _buildplatform_arg() -> List[str]:
    # docker/Dockerfile declares a global `ARG BUILDPLATFORM` without a value; current BuildKit
    # then resolves `FROM --platform=$BUILDPLATFORM` to "" and fails. Pass the native platform.
    return ["--build-arg", f"BUILDPLATFORM=linux/{docker.native_arch()}"]


def build_image(root: Path, testing: bool = False, push: bool = False, save_tar: bool = True) -> None:
    """Build the image. push=True builds multi-arch with buildx and pushes; otherwise native-only + tar."""
    if not docker.available():
        raise CommandError(["docker"], 127, "'docker' not found on PATH")
    build_arg, tags = image_tags(root, testing)
    label = "testing" if testing else tags[0].rsplit(":", 1)[1]
    ui.step(f"Building Docker image for MonsterMQ Edge ({label})...")
    # The Go build inside the image embeds internal/dashboard/dist (gitignored), so it must be
    # prepared first - otherwise a fresh checkout (e.g. CI) ships an image without a dashboard.
    prepare_dashboard(root)
    buildx = docker.buildx()

    if push:
        if buildx is None:
            raise CommandError(["docker", "buildx"], 1, "docker buildx is required for multi-arch publish")
        if not succeeds([*buildx, "inspect", BUILDER_NAME]):
            ui.step(f"Creating buildx builder '{BUILDER_NAME}'...")
            run([*buildx, "create", "--name", BUILDER_NAME, "--use"], cwd=root)
        else:
            run([*buildx, "use", BUILDER_NAME], cwd=root)
        ui.step(f"Building and pushing multi-arch images ({platforms()})...")
        run([*buildx, "build", "--platform", platforms(), "--push", *_buildplatform_arg(),
             "--build-arg", f"VERSION={build_arg}",
             *_tag_args(tags), "-f", "docker/Dockerfile", "."], cwd=root)
        ui.ok("Multi-arch images built and pushed")
        for tag in tags:
            ui.detail(f"- {tag}")
        return

    env = None
    if buildx is not None:
        cmd = [*buildx, "build", "--load", *_buildplatform_arg()]
    else:
        ui.warn("buildx not available, falling back to docker build...")
        arch = docker.native_arch()
        # The image always contains a Linux binary, whatever the host OS is.
        cmd = ["docker", "build", "--build-arg", f"BUILDPLATFORM=linux/{arch}",
               "--build-arg", "TARGETOS=linux", "--build-arg", f"TARGETARCH={arch}"]
        env = {"DOCKER_BUILDKIT": "1"}
    ui.step("Building Docker image (local, native platform only)...")
    run([*cmd, "--build-arg", f"VERSION={build_arg}", *_tag_args(tags), "-f", "docker/Dockerfile", "."],
        cwd=root, env=env)
    ui.ok("Docker image built successfully (native platform)")
    for tag in tags:
        ui.detail(f"- {tag}")
    if save_tar:
        tar = root / "docker" / TAR_NAME
        ui.step(f"Saving image to {tar} ...")
        run(["docker", "image", "save", "-o", str(tar), tags[0]], cwd=root)
        ui.ok(f"Image saved to docker/{TAR_NAME}")
    ui.info("Image built for native platform only. To build multi-arch and push, run with -y")


def _ask_push() -> bool:
    if not interactive():
        ui.warn("no terminal to ask on; building locally (pass -y to push, -n to silence this)")
        return False
    try:
        answer = input("Do you want to build multi-arch and push to Docker Hub? [y/N] ").strip().lower()
    except EOFError:
        print()
        return False
    return answer in ("y", "yes")


def main(root: Path, argv: List[str]) -> int:
    p = parser("docker", "Build the MonsterMQ Edge Docker image (docker/Dockerfile).",
               epilog="Without -n or -y, asks whether to publish (no terminal: local build).\n"
                      "Local builds build the native platform only and save docker/monstermq-edge.tar.\n"
                      f"Publishing builds multi-arch ({platforms()}) and pushes to Docker Hub.\n"
                      "Environment: IMAGE_REPO, PLATFORMS, FORCE_NO_BUILDX=true.")
    p.add_argument("-t", "--testing", action="store_true", help="build the testing image (tag: testing)")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("-n", dest="push", action="store_false", default=None,
                      help="do not publish (local build only)")
    mode.add_argument("-y", dest="push", action="store_true", default=None,
                      help="multi-arch build and push to Docker Hub without asking")
    p.add_argument("--no-tar", dest="save_tar", action="store_false", help="skip saving the image as a tar file")
    args = p.parse_args(argv)
    push = args.push if args.push is not None else _ask_push()
    build_image(root, testing=args.testing, push=push, save_tar=args.save_tar)
    return 0


def commands(root: Path):
    return {"docker": Command("build the Docker image (local, or -y: multi-arch + push)",
                              lambda argv: main(root, argv))}
