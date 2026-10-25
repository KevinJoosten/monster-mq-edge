"""make.py publish - upload Debian packages to the GitHub release and push Docker images.

Nothing is uploaded until every check for every selected target has passed:
the version is a release version (GitHub), the release tag exists on origin and
points at HEAD, gh is installed and logged in, the .deb files exist, and Docker
with buildx is available. The tag is never created or pushed here - that is
'make.py release' / 'make.py retag'.
"""

from pathlib import Path
from typing import List

from mmqlib import docker, github, ui
from mmqlib.cli import Command
from mmqlib.git import Git, GitError

from . import image
from .build import DEB_ARCHES
from .common import is_dev_version, parser, read_version

REMOTE = "origin"


def deb_files(root: Path, version: str) -> List[Path]:
    return [root / "bin" / f"monstermq-edge_{version}_{arch}.deb" for arch in DEB_ARCHES]


def check_tag_at_head(root: Path, tag: str) -> List[str]:
    """Problems with the release tag on origin (empty when it exists and points at HEAD)."""
    git = Git(root)
    try:
        head = git.head()
        remote = git.remote_tags(REMOTE)
    except GitError as e:
        return [f"cannot read the tags on {REMOTE}: {e}"]
    ref = remote.get(tag)
    if ref is None:
        return [f"tag {tag} does not exist on {REMOTE}. Create the release first: python make.py release"]
    if ref.commit != head:
        return [f"tag {tag} on {REMOTE} points at {ref.commit[:7]}, but HEAD is {head[:7]}. "
                f"Check out the tagged commit, or move the tag with: python make.py retag"]
    return []


def main(root: Path, argv: List[str]) -> int:
    p = parser("publish", "Publish MonsterMQ Edge: Debian packages to the GitHub release, images to Docker Hub.",
               epilog="Development versions (X.Y.Z-dev) can only be published as Docker image ':testing'.")
    p.add_argument("--all", action="store_true", help="publish both GitHub release and Docker Hub images")
    p.add_argument("--github", action="store_true", help="publish Debian packages to the GitHub release only")
    p.add_argument("--docker", action="store_true", help="build multi-arch Docker image and push to Docker Hub only")
    p.add_argument("-t", "--testing", action="store_true",
                   help="build multi-arch Docker image tagged 'testing' and push it to Docker Hub")
    p.add_argument("-y", "--yes", action="store_true", help="publish without asking")
    if not argv:
        p.print_help()
        return 0
    args = p.parse_args(argv)

    publish_github = args.all or args.github
    publish_docker = args.all or args.docker or args.testing
    testing = args.testing
    if not (publish_github or publish_docker):  # e.g. only -y
        publish_github = publish_docker = True

    version = read_version(root)
    tag = f"v{version}"

    if is_dev_version(version):
        if publish_github:
            ui.error(f"Current version ({version}) is a development version.")
            ui.info("GitHub releases cannot be published for development versions.")
            ui.info("Finalize the release first:  python make.py release")
            ui.info("To publish a development Docker image with label 'testing', run:")
            ui.info("  python make.py publish --docker   (or: python make.py publish --testing)")
            return 1
        testing = True  # dev versions go to Docker Hub as ':testing' only

    # ---- preflight: nothing is uploaded unless all of this passes -------------
    problems: List[str] = []
    debs = deb_files(root, version)
    if publish_github:
        if not github.available():
            problems.append("GitHub CLI ('gh') is not installed")
        elif not github.authenticated():
            problems.append("GitHub CLI is not authenticated. Run 'gh auth login'")
        missing = [d for d in debs if not d.is_file()]
        if missing:
            problems.append("Debian packages missing (build them with: python make.py build --deb):\n"
                            + "\n".join(f"    - {d.relative_to(root).as_posix()}" for d in missing))
    if publish_github or (publish_docker and not testing):
        problems += check_tag_at_head(root, tag)
    if publish_docker:
        if not docker.daemon_running():
            problems.append("docker is not on PATH or the Docker daemon is not running")
        elif docker.buildx() is None:
            problems.append("docker buildx is required for multi-arch publish")
    if problems:
        for problem in problems:
            ui.error(problem)
        ui.info("Nothing was published.")
        return 1

    # ---- plan + confirm ------------------------------------------------------
    if testing:
        target = f"{image.image_repo()}:testing"
        ui.header(f"MonsterMQ Edge Publish Pipeline (Target: {target})")
    else:
        ui.header(f"MonsterMQ Edge Publish Pipeline (Target: {tag})")
    ui.info("Selected publish targets:")
    if publish_github:
        ui.info(f"  - Debian packages -> GitHub release {tag}")
    if publish_docker:
        _, tags = image.image_tags(root, testing)
        ui.info(f"  - Docker Hub multi-arch images ({image.platforms()}) -> {', '.join(tags)}")
    if not testing and Git(root).status_lines():
        ui.warn("the working tree has uncommitted changes; the Docker image is built from the working tree")
    print()
    ui.confirm(f"Publish {'the testing image' if testing else tag} now?", assume_yes=args.yes)

    # ---- publish -------------------------------------------------------------
    if publish_github:
        ui.step(f"[1/2] Publishing Debian packages to GitHub release {tag}...")
        notes = root / "releases" / f"{tag}.txt"
        github.publish_assets(tag, [d.relative_to(root).as_posix() for d in debs], cwd=root,
                              title=f"Release {version}", notes_file=notes if notes.is_file() else None)
        ui.ok(f"Debian packages published to GitHub release {tag}")

    if publish_docker:
        ui.step(f"[2/2] Publishing multi-arch Docker image{' (testing)' if testing else 's'} to Docker Hub...")
        image.build_image(root, testing=testing, push=True)
        ui.ok("Docker Hub images published")

    ui.header("Publish Complete")
    return 0


def commands(root: Path):
    return {"publish": Command("upload .deb packages to the GitHub release and push Docker images",
                               lambda argv: main(root, argv))}
