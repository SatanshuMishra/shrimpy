import argparse
import shutil
import subprocess
import sys
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(repo_root))

from scripts.extract import extract_resources  # noqa: E402


RENDERER_REPO_URL = "https://github.com/WoWs-Builder-Team/minimap_renderer.git"
GP2JSON_REPO_URL = "https://github.com/WoWs-Builder-Team/FilteredGameParams2Json.git"


def _run(cmd: list[str], cwd: Path | None = None) -> None:
    subprocess.run(cmd, cwd=str(cwd) if cwd else None, check=True)


def _ensure_repo(path: Path, url: str, update: bool) -> None:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        _run(["git", "clone", url, str(path)])
        return
    if update:
        _run(["git", "-C", str(path), "pull", "--ff-only"])


def _copy_tree(src: Path, dest: Path) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        shutil.rmtree(dest)
    shutil.copytree(src, dest)


def _copy_gameparams_to_renderer_repo(gameparams: Path, renderer_repo: Path) -> None:
    candidates = []
    for path in renderer_repo.rglob("GameParams.data"):
        candidates.append(path)

    if not candidates:
        target = renderer_repo / "renderer_data" / "gameparams" / "GameParams.data"
        target.parent.mkdir(parents=True, exist_ok=True)
        candidates = [target]

    for target in candidates:
        shutil.copy2(gameparams, target)


def _run_create_data(renderer_repo: Path, python_exe: Path | None) -> None:
    cmd = [str(python_exe or sys.executable), "create_data.py"]
    _run(cmd, cwd=renderer_repo)


def _detect_renderer_generated_target() -> Path | None:
    try:
        import renderer
    except Exception:
        return None

    pkg_dir = Path(renderer.__file__).parent
    candidates = [
        pkg_dir / "generated",
        pkg_dir.parent / "generated",
        pkg_dir / "data" / "generated",
    ]
    for candidate in candidates:
        if candidate.exists():
            return candidate

    # Fallback: create a generated folder in package
    target = pkg_dir / "generated"
    target.mkdir(parents=True, exist_ok=True)
    return target


def _latest_bin(bin_root: Path) -> str:
    candidates = [p for p in bin_root.iterdir() if p.is_dir() and p.name.isdigit()]
    if not candidates:
        raise RuntimeError(f"No bin versions found in {bin_root}")
    return max(candidates, key=lambda p: int(p.name)).name


def main() -> None:
    parser = argparse.ArgumentParser(
        description="End-to-end WoWS resource update pipeline."
    )
    parser.add_argument("--wows-root", required=True, help="WoWS installation root.")
    parser.add_argument(
        "--bin",
        default=None,
        help="Bin version to use (default: latest in WoWS/bin).",
    )
    parser.add_argument(
        "--unpacker",
        default=None,
        help="Path to wowsunpack.exe (default: <wows-root>/wowsunpack.exe).",
    )
    parser.add_argument(
        "--minimap-repo",
        default=str(repo_root / "tools" / "minimap_renderer"),
        help="Path to minimap_renderer repo clone.",
    )
    parser.add_argument(
        "--gp2json-repo",
        default=str(repo_root / "tools" / "FilteredGameParams2Json"),
        help="Path to FilteredGameParams2Json repo clone.",
    )
    parser.add_argument(
        "--renderer-target",
        default=None,
        help="Path to renderer/generated in installed package (auto-detect if omitted).",
    )
    parser.add_argument(
        "--python",
        dest="python_exe",
        default=None,
        help="Python executable to use for create_data.py and GameParams2Json.py.",
    )
    parser.add_argument(
        "--no-git-update",
        action="store_true",
        help="Do not clone/pull external repos (assume already present).",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip WoWS unpacking (assume res_extract already exists).",
    )
    parser.add_argument(
        "--skip-renderer-data",
        action="store_true",
        help="Skip minimap_renderer generated data update.",
    )
    parser.add_argument(
        "--skip-ships",
        action="store_true",
        help="Skip ships.json generation.",
    )
    args = parser.parse_args()

    wows_root = Path(args.wows_root).resolve()
    res_extract = wows_root / "res_extract"
    unpacker_path = (
        Path(args.unpacker).resolve()
        if args.unpacker
        else wows_root / "wowsunpack.exe"
    )
    python_exe = Path(args.python_exe).resolve() if args.python_exe else None

    if not args.skip_extract:
        extract_resources(
            wows_root=wows_root,
            bin_num=args.bin or _latest_bin(wows_root / "bin"),
            output_dir=res_extract,
            unpacker_path=unpacker_path,
        )

    gameparams_data = res_extract / "content" / "GameParams.data"
    texts_dir = res_extract / "texts"
    silhouettes_dir = res_extract / "gui" / "ships_silhouettes"

    resources_dir = repo_root / "resources"
    assets_dir = repo_root / "bot" / "assets" / "public"
    resources_dir.mkdir(parents=True, exist_ok=True)
    assets_dir.mkdir(parents=True, exist_ok=True)

    if gameparams_data.exists():
        shutil.copy2(gameparams_data, resources_dir / "GameParams.data")
    else:
        raise FileNotFoundError(f"Missing {gameparams_data}")

    if texts_dir.exists():
        _copy_tree(texts_dir, resources_dir / "texts")

    if silhouettes_dir.exists():
        _copy_tree(silhouettes_dir, assets_dir / "ships_silhouettes")

    if not args.skip_renderer_data:
        minimap_repo = Path(args.minimap_repo).resolve()
        if not args.no_git_update:
            _ensure_repo(minimap_repo, RENDERER_REPO_URL, update=True)

        _copy_gameparams_to_renderer_repo(gameparams_data, minimap_repo)
        _run_create_data(minimap_repo, python_exe)

        generated_src = minimap_repo / "generated"
        if not generated_src.exists():
            raise FileNotFoundError(f"Missing generated data: {generated_src}")

        if args.renderer_target:
            renderer_target = Path(args.renderer_target).resolve()
            renderer_target.mkdir(parents=True, exist_ok=True)
        else:
            renderer_target = _detect_renderer_generated_target()
            if renderer_target is None:
                raise RuntimeError(
                    "Could not locate renderer package. Use --renderer-target."
                )

        _copy_tree(generated_src, renderer_target)

    if not args.skip_ships:
        gp2json_repo = Path(args.gp2json_repo).resolve()
        if not args.no_git_update:
            _ensure_repo(gp2json_repo, GP2JSON_REPO_URL, update=True)

        gameparams2json = gp2json_repo / "GameParams2Json.py"
        if not gameparams2json.exists():
            raise FileNotFoundError(f"Missing {gameparams2json}")

        cmd = [
            str(python_exe or sys.executable),
            str(gameparams2json),
            "--path",
            str(gameparams_data),
        ]
        _run(cmd, cwd=gp2json_repo)

        entities_dir = gp2json_repo / "live" / "entities"
        if not entities_dir.exists():
            raise FileNotFoundError(f"Missing entities output: {entities_dir}")

        generate_script = repo_root / "scripts" / "ships" / "generate.py"
        _run(
            [
                str(python_exe or sys.executable),
                str(generate_script),
                "--entities",
                str(entities_dir),
                "--texts",
                str(resources_dir / "texts"),
            ],
            cwd=repo_root,
        )

        generated_ships = repo_root / "generated" / "ships.json"
        if generated_ships.exists():
            shutil.copy2(generated_ships, assets_dir / "ships.json")


if __name__ == "__main__":
    main()
