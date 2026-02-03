import argparse
import functools
import json
import os
from pathlib import Path

import polib

ENCODING = "latin1"
KEYS = (
    "id",
    "index",
    "isPaperShip",
    "group",
    "level",
    "name",
    "typeinfo.species",
    "typeinfo.nation",
)
DEFAULT_GAMEPARAMS_PATH = Path(__file__).resolve().parents[2] / "resources" / "GameParams.json"
DEFAULT_OUTPUT_PATH = Path(__file__).resolve().parents[2] / "generated" / "ships.json"
DEFAULT_TEXTS_PATH = Path(__file__).resolve().parents[2] / "resources" / "texts"

translations = {}


def rgetattr(obj, attr, *args):
    """
    https://stackoverflow.com/questions/31174295/getattr-and-setattr-on-nested-objects
    """

    def _getattr(_obj, _attr):
        return getattr(_obj, _attr, *args)

    return functools.reduce(_getattr, [obj] + attr.split("."))


def get_translations(index: str) -> dict[str, dict[str, str]]:
    return {
        locale: {
            "short": messages.get(f"IDS_{index}", None),
            "full": messages.get(f"IDS_{index}_FULL", None),
        }
        for locale, messages in translations.items()
    }


def _load_translations(texts_path: Path) -> None:
    print("Loading translations...")
    for locale in sorted(os.listdir(texts_path)):
        locale_dir = texts_path / locale
        if locale_dir.is_dir():
            mo_file = polib.mofile(locale_dir / "LC_MESSAGES" / "global.mo")
            translations[locale] = {entry.msgid: entry.msgstr for entry in mo_file}

def _load_gameparams_from_json(gameparams_path: Path) -> dict:
    print(f"Loading GameParams JSON: {gameparams_path}")
    with open(gameparams_path, "r", encoding="utf-8") as fp:
        return json.load(fp)


def _load_gameparams_from_entities(entities_path: Path) -> dict:
    print(f"Loading GameParams entities: {entities_path}")
    ship_dir = entities_path / "Ship"
    if ship_dir.exists():
        candidates = sorted(ship_dir.glob("*.json"))
    else:
        candidates = sorted(entities_path.rglob("Ship/*.json"))

    gp_data: dict = {}
    for path in candidates:
        try:
            with open(path, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            if isinstance(data, list):
                for entity in data:
                    if isinstance(entity, dict):
                        index = entity.get("index")
                        if index:
                            gp_data[index] = entity
            elif isinstance(data, dict):
                index = data.get("index")
                if index:
                    gp_data[index] = data
        except json.JSONDecodeError:
            continue

    if not gp_data:
        raise RuntimeError(
            "No ship entities found. Ensure entities output contains Ship JSON files."
        )
    return gp_data


def main():
    parser = argparse.ArgumentParser(description="Generate ships.json from GameParams.")
    parser.add_argument(
        "--gameparams-json",
        default=str(DEFAULT_GAMEPARAMS_PATH),
        help="Path to GameParams.json (preferred when available).",
    )
    parser.add_argument(
        "--entities",
        default=None,
        help="Path to GameParams entities folder (e.g., live/entities).",
    )
    parser.add_argument(
        "--texts",
        default=str(DEFAULT_TEXTS_PATH),
        help="Path to WoWS texts folder (default: resources/texts).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_PATH),
        help="Output path for generated ships.json.",
    )
    args = parser.parse_args()

    texts_path = Path(args.texts).resolve()
    output_path = Path(args.output).resolve()

    _load_translations(texts_path)

    gp_data = None
    gameparams_json = Path(args.gameparams_json).resolve()
    if gameparams_json.exists():
        gp_data = _load_gameparams_from_json(gameparams_json)
    elif args.entities:
        gp_data = _load_gameparams_from_entities(Path(args.entities).resolve())
    else:
        raise RuntimeError(
            "GameParams.json not found and --entities not provided."
        )

    ships = []
    for index, entity in gp_data.items():
        # Check if this is a Ship type
        if entity.get("typeinfo", {}).get("type") != "Ship":
            continue
            
        data = {
            "id": entity.get("id"),
            "index": entity.get("index"),
            "isPaperShip": entity.get("isPaperShip", False),
            "group": entity.get("group"),
            "level": entity.get("level"),
            "name": entity.get("name"),
            "species": entity.get("typeinfo", {}).get("species"),
            "nation": entity.get("typeinfo", {}).get("nation"),
        }
        data["translations"] = get_translations(entity.get("index", ""))
        ships.append(data)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as fp:
        json.dump(ships, fp, indent=4)

    print(f"Done! Wrote {output_path}")


if __name__ == "__main__":
    main()
