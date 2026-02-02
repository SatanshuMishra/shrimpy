import functools
import json
import os
import pickle
import struct
import zlib

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
GAMEPARAMS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../../resources/GameParams.json"
)
OUTPUT_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../../generated/ships.json"
)
TEXTS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "../../resources/texts"
)

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


def main():
    print("Loading translations...")
    for locale in sorted(os.listdir(TEXTS_PATH)):
        if os.path.isdir(f"{TEXTS_PATH}/{locale}"):
            mo_file = polib.mofile(f"{TEXTS_PATH}/{locale}/LC_MESSAGES/global.mo")
            translations[locale] = {entry.msgid: entry.msgstr for entry in mo_file}

    print("Loading GameParams...")
    with open(GAMEPARAMS_PATH, "r", encoding="utf-8") as fp:
        gp_data: dict = json.load(fp)

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

    with open(OUTPUT_PATH, "w") as fp:
        json.dump(ships, fp, indent=4)

    print("Done!")


if __name__ == "__main__":
    main()
