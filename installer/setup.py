"""
Copy bundled WinSW from installer/bin/ into service/winsw.exe for Inno Setup packaging.

Does not download from the network. Skips the copy if the destination already exists.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parent.parent


def copy_winsw(*, force: bool = False) -> Path:
    root = _repo_root()
    src = root / "installer" / "bin" / "WinSW.exe"
    dst = root / "service" / "winsw.exe"

    if dst.exists() and not force:
        print(f"WinSW already present at {dst}, skipping copy.", file=sys.stderr)
        return dst

    if not src.is_file():
        raise FileNotFoundError(
            f"Bundled WinSW missing: {src}. "
            "Place WinSW x64 as installer/bin/WinSW.exe (e.g. rename WinSW-x64.exe from "
            "https://github.com/winsw/winsw/releases )."
        )

    dst.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, dst)
    print(f"Copied WinSW to {dst}", file=sys.stderr)
    return dst


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Stage WinSW for Fetcher installer build.")
    p.add_argument(
        "--force",
        action="store_true",
        help="Copy even if service/winsw.exe already exists.",
    )
    args = p.parse_args(argv)
    try:
        copy_winsw(force=args.force)
    except FileNotFoundError as e:
        print(e, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
