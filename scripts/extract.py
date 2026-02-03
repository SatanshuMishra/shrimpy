"""
This script should be placed in the root of a WoWS installation.
It will generate and populate res_extract.

Make sure you have the unpacker:
https://forum.worldofwarships.eu/topic/113847-all-wows-unpack-tool-unpack-game-client-resources/
"""

import argparse
import os
import shutil
import subprocess
from pathlib import Path

OUTPUT_NAME = "res_extract"
UNPACK_LIST = (
    "content/GameParams.data",
    "gui/ships_silhouettes/*",
)


def _build_include_args():
    include = []
    for pattern in UNPACK_LIST:
        include.append("-I")
        include.append(pattern)
    return include


def extract_resources(
    wows_root: Path,
    bin_num: str,
    output_dir: Path,
    unpacker_path: Path,
) -> Path:
    wows_root = Path(wows_root)
    output_dir = Path(output_dir)
    unpacker_path = Path(unpacker_path)

    bin_path = wows_root / "bin" / str(bin_num)
    idx_path = bin_path / "idx"
    pkg_path = wows_root / "res_packages"
    output_dir.mkdir(parents=True, exist_ok=True)

    include = _build_include_args()

    subprocess.run(
        [
            str(unpacker_path),
            "-x",
            str(idx_path),
            "-p",
            str(pkg_path),
            "-o",
            str(output_dir),
            *include,
        ],
        check=True,
    )

    texts_src = bin_path / "res" / "texts"
    texts_dest = output_dir / "texts"
    if texts_dest.exists():
        shutil.rmtree(texts_dest)
    shutil.copytree(texts_src, texts_dest)

    print(f"Extraction complete: {output_dir}")
    return output_dir


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Extracts game resources.")
    parser.add_argument(
        "--wows-root",
        default=".",
        help="WoWS installation root (default: current directory).",
    )
    parser.add_argument(
        "--bin",
        default=None,
        help="The game version to use (default: latest in bin/).",
    )
    parser.add_argument(
        "--out",
        default=OUTPUT_NAME,
        help=f"Output folder name (default: {OUTPUT_NAME}).",
    )
    parser.add_argument(
        "--unpacker",
        default="wowsunpack.exe",
        help="Path to wowsunpack.exe (default: wowsunpack.exe in WoWS root).",
    )
    args = parser.parse_args()

    wows_root = Path(args.wows_root).resolve()
    bin_root = wows_root / "bin"
    if args.bin:
        bin_num = args.bin
    else:
        candidates = [
            p for p in bin_root.iterdir() if p.is_dir() and p.name.isdigit()
        ]
        bin_num = max(candidates, key=lambda p: int(p.name)).name if candidates else None
    if not bin_num:
        raise SystemExit("Could not determine bin version. Use --bin.")

    unpacker_path = Path(args.unpacker)
    if not unpacker_path.is_absolute():
        unpacker_path = wows_root / unpacker_path

    extract_resources(
        wows_root=wows_root,
        bin_num=bin_num,
        output_dir=wows_root / args.out,
        unpacker_path=unpacker_path,
    )
