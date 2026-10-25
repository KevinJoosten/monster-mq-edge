"""GitHub CLI (gh) helpers for release assets."""

from pathlib import Path
from typing import Optional, Sequence

from .proc import PathLike, run, succeeds, which


def available() -> bool:
    return which("gh") is not None


def authenticated() -> bool:
    return available() and succeeds(["gh", "auth", "status"])


def release_exists(tag: str, cwd: PathLike) -> bool:
    return succeeds(["gh", "release", "view", tag], cwd=cwd)


def publish_assets(tag: str, assets: Sequence[PathLike], cwd: PathLike, title: str,
                   notes_file: Optional[Path] = None) -> None:
    """Create the release for an existing tag, or replace the assets of an existing release.

    Safe to re-run: an existing release gets its assets re-uploaded with --clobber.
    --verify-tag makes gh refuse to create the tag itself.
    """
    files = [str(a) for a in assets]
    if release_exists(tag, cwd):
        run(["gh", "release", "upload", tag, *files, "--clobber"], cwd=cwd)
        return
    notes = ["--notes-file", str(notes_file)] if notes_file else ["--generate-notes"]
    run(["gh", "release", "create", tag, *files, "--verify-tag", "--title", title, *notes], cwd=cwd)
