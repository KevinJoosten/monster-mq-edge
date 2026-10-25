"""Docker / buildx helpers (detection only; builds are composed by the caller)."""

import os
import platform
from typing import List, Optional

from .proc import succeeds, which


def available() -> bool:
    return which("docker") is not None


def daemon_running() -> bool:
    """True when the docker CLI can reach a daemon."""
    return available() and succeeds(["docker", "info"])


def buildx() -> Optional[List[str]]:
    """The buildx command prefix: standalone 'docker-buildx' (macOS) or 'docker buildx'.

    FORCE_NO_BUILDX=true disables buildx, as in the old docker/build.sh.
    """
    if os.environ.get("FORCE_NO_BUILDX", "false") == "true":
        return None
    if which("docker-buildx"):
        return ["docker-buildx"]
    if available() and succeeds(["docker", "buildx", "version"]):
        return ["docker", "buildx"]
    return None


def native_arch() -> str:
    """The host CPU as a Docker/Go architecture name (amd64, arm64, arm)."""
    machine = platform.machine().lower()
    return {
        "x86_64": "amd64", "amd64": "amd64",
        "aarch64": "arm64", "arm64": "arm64",
        "armv7l": "arm", "armhf": "arm",
    }.get(machine, machine)
