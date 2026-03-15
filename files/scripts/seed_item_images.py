#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Seed item images by looking up photos from Wikimedia Commons.

Strategy
--------
1. Strip the leading product-code prefix (before the first " - " or " – ")
2. Strip trailing size/grade indicators (22+, >18C, 3KG, XL, etc.)
3. Take the first 1-2 words as the "core vegetable" name
4. Translate Dutch core names to English using the built-in map
5. Look up ONE Wikipedia thumbnail per unique core name, then bulk-update
   all matching items  →  54k rows = ~200 API calls.

Usage:
  cd files
  python scripts/seed_item_images.py [--dry-run] [--overwrite] [--schema STR]
"""

import argparse
import re
import sys
import time
import urllib.parse
import urllib.request
import json
from pathlib import Path
from collections import defaultdict

# Force UTF-8 on Windows console
if hasattr(sys.stdout, 'buffer'):
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

_files_dir = Path(__file__).resolve().parent.parent
if str(_files_dir) not in sys.path:
    sys.path.insert(0, str(_files_dir))

try:
    import yaml
    import psycopg2
    import psycopg2.extras
except ImportError as e:
    print(f"Missing dependency: {e}")
    sys.exit(1)

_CONFIG_YAML = _files_dir / "config" / "config.yaml"

# ── Dutch -> English vegetable/fruit/herb name map ────────────────────────────
NL_TO_EN: dict[str, str] = {
    # Vegetables
    "ABRIKOOS":         "Apricot",
    "AARDAPPEL":        "Potato",
    "ZOETE AARDAPPEL":  "Sweet potato",
    "AJUIN":            "Onion",
    "UI":               "Onion",
    "UIEN":             "Onion",
    "ANDIJVIE":         "Endive",
    "ARTISJOK":         "Artichoke",
    "ASPERGE":          "Asparagus",
    "AUBERGINE":        "Eggplant",
    "AVOCADO":          "Avocado",
    "BATAAT":           "Sweet potato",
    "BIESLOOK":         "Chives",
    "BIET":             "Beetroot",
    "BLKSELDERIJ":      "Celery",
    "BLOEMKOOL":        "Cauliflower",
    "BOERENKOOL":       "Kale",
    "BOSUI":            "Scallion",
    "BROCCOLI":         "Broccoli",
    "CHAMPIGNON":       "Mushroom",
    "CHINESE KOOL":     "Napa cabbage",
    "CITROEN":          "Lemon",
    "COURGETTE":        "Zucchini",
    "DAIKON":           "Daikon radish",
    "DILLE":            "Dill",
    "ERWT":             "Pea",
    "ERWTEN":           "Pea",
    "FENEGRIEK":        "Fenugreek",
    "VENKEL":           "Fennel",
    "GEMBER":           "Ginger",
    "GROENE PAPRIKA":   "Green bell pepper",
    "GROENE PEPER":     "Green pepper",
    "JALAPENO":         "Jalapeño",
    "JALAPEÑO":         "Jalapeño",
    "KNOFLOOK":         "Garlic",
    "KOOL":             "Cabbage",
    "KOMKOMMER":        "Cucumber",
    "KORIANDER":        "Coriander",
    "PREI":             "Leek",
    "LEEK":             "Leek",
    "MAIS":             "Corn",
    "MAÏS":             "Corn",
    "MANGO":            "Mango",
    "MANGETOUT":        "Snow pea",
    "PAK CHOI":         "Bok choy",
    "PAPRIKA":          "Bell pepper",
    "PASTINAAK":        "Parsnip",
    "PETERSELIE":       "Parsley",
    "PEUL":             "Snow pea",
    "PEULTJE":          "Snow pea",
    "POMPOEN":          "Pumpkin",
    "POSTELEIN":        "Purslane",
    "RAAP":             "Turnip",
    "RADIJS":           "Radish",
    "RETTICH":          "Daikon radish",
    "RODE BIET":        "Beetroot",
    "RODE KOOL":        "Red cabbage",
    "RODE PAPRIKA":     "Red bell pepper",
    "RUCOLA":           "Arugula",
    "SAVOOIEKOOL":      "Savoy cabbage",
    "SELDERIJ":         "Celery",
    "SJALOT":           "Shallot",
    "SJALOTTEN":        "Shallot",
    "SNIJBOON":         "Runner bean",
    "SPINAZIE":         "Spinach",
    "SPRUITJES":        "Brussels sprout",
    "SPITSKOOL":        "Pointed cabbage",
    "SUGARSNAP":        "Snap pea",
    "SUIKERMAIS":       "Sweetcorn",
    "TOMAAT":           "Tomato",
    "TOMATEN":          "Tomato",
    "TUINBOON":         "Broad bean",
    "WATERKERS":        "Watercress",
    "WITLOF":           "Chicory",
    "WITTE ASPERGE":    "White asparagus",
    "WITTE KOOL":       "White cabbage",
    "WORTEL":           "Carrot",
    "WORTELEN":         "Carrot",
    "ZOETE MAÏS":       "Sweetcorn",
    "ZUCCHINI":         "Zucchini",
    # Fruit
    "AARDBEI":          "Strawberry",
    "AARDBEIEN":        "Strawberry",
    "ANANAS":           "Pineapple",
    "APPEL":            "Apple",
    "BANAAN":           "Banana",
    "DRUIF":            "Grape",
    "DRUIVEN":          "Grape",
    "KERS":             "Cherry",
    "KIWI":             "Kiwifruit",
    "KWEEPEER":         "Quince",
    "LIMOEN":           "Lime",
    "MELOEN":           "Melon",
    "NECTARINE":        "Nectarine",
    "PEER":             "Pear",
    "PERZIK":           "Peach",
    "PRUIM":            "Plum",
    "SINAASAPPEL":      "Orange",
    "WATERMELOEN":      "Watermelon",
    # Herbs
    "BASILICUM":        "Basil",
    "DRAGON":           "Tarragon",
    "MUNT":             "Mint",
    "OREGANO":          "Oregano",
    "ROZEMARIJN":       "Rosemary",
    "TIJM":             "Thyme",
    # English names that need no translation but benefit from direct mapping
    "HONEY CRUNCH":     "Honeycrisp apple",
    "JONAGOLD":         "Jonagold apple",
}

# Suffixes/words that indicate size, grade, or variety qualifiers — strip them
_STRIP_SUFFIXES = re.compile(
    r'[\s,]+(?:'
    r'\d[\d\+\-\*\.]*(?:KG|G|GR|GRAM|LTR|L|ML|ST|STUK|CM|MM|PC|PR)?'  # 1KG, 500G, 22+
    r'|[<>]=?\d[\d\.]*(?:CM|MM|KG|G|C)?'    # >18C, <20CM
    r'|[XLSM]{1,3}'                          # XL, L, M, S
    r'|[A-Z]\d{1,2}'                         # A1, B2
    r'|(?:KLASSE|CLASS|GLAS|KAS|IMPORT|NL|BE|FR|ES|IT|DE|MAR|EG|SA|ZA|US|MX)'
    r')[\s\S]*$',
    re.IGNORECASE,
)

# Leading product code: anything up to first " - " or " – "
_CODE_PREFIX = re.compile(r'^.+?(?:\s[-–]\s|\s{1,3}[-–]\s{1,3})(?=[A-Z])', re.UNICODE)


def extract_core(raw: str) -> str:
    """Return the 1-2 word core vegetable/fruit name from a raw item name."""
    name = raw.strip().upper()

    # Strip leading product code if " - " separator is present
    m = _CODE_PREFIX.match(name)
    if m:
        name = name[m.end():]

    # Strip trailing size/grade/qualifier suffixes
    name = _STRIP_SUFFIXES.sub('', name).strip()

    # Take first two words max (captures "RODE KOOL", "ZOETE AARDAPPEL" etc.)
    words = name.split()
    two = ' '.join(words[:2]) if len(words) >= 2 else name

    # Prefer 2-word match, fall back to 1-word
    if two.upper() in NL_TO_EN:
        return two
    if words and words[0].upper() in NL_TO_EN:
        return words[0]
    # No translation — return first 2 words as-is (may still be English)
    return two if two else name


def to_english(core: str) -> str:
    return NL_TO_EN.get(core.upper(), core)


# ── Wikipedia thumbnail ───────────────────────────────────────────────────────
_WIKI_API   = "https://en.wikipedia.org/w/api.php"
_THUMB_SIZE = 80
_DELAY      = 0.35


def _wiki_get(url: str, retries: int = 3) -> bytes | None:
    req = urllib.request.Request(url, headers={"User-Agent": "ForecastAI-ImageSeeder/1.0"})
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                return resp.read()
        except Exception as exc:
            msg = str(exc)
            if "429" in msg:
                wait = 2 ** (attempt + 2)   # 4s, 8s, 16s
                print(f" [rate-limited, retry in {wait}s]", end="", flush=True)
                time.sleep(wait)
            else:
                print(f" [err: {exc}]", end="")
                return None
    return None


def _wiki_thumbnail(title: str) -> str | None:
    params = {
        "action": "query", "titles": title, "prop": "pageimages",
        "format": "json", "pithumbsize": str(_THUMB_SIZE), "pilicense": "any",
    }
    data_bytes = _wiki_get(_WIKI_API + "?" + urllib.parse.urlencode(params))
    if not data_bytes:
        return None
    try:
        data = json.loads(data_bytes)
        for page in data.get("query", {}).get("pages", {}).values():
            src = page.get("thumbnail", {}).get("source")
            if src:
                return src
    except Exception:
        pass
    return None


def _wiki_search_first(query: str) -> str | None:
    params = {"action": "opensearch", "search": query, "limit": "1", "format": "json"}
    data_bytes = _wiki_get(_WIKI_API + "?" + urllib.parse.urlencode(params))
    if not data_bytes:
        return None
    try:
        results = json.loads(data_bytes)
        titles = results[1] if len(results) > 1 else []
        if titles:
            time.sleep(_DELAY)
            return _wiki_thumbnail(titles[0])
    except Exception:
        pass
    return None


def fetch_image(en_name: str) -> str | None:
    url = _wiki_thumbnail(en_name)
    if url:
        return url
    time.sleep(_DELAY)
    return _wiki_search_first(en_name)


# ── Config / DB ───────────────────────────────────────────────────────────────

def get_pg(cfg: dict) -> dict:
    db = cfg.get("database", {})
    return {
        "host": db.get("host", "localhost"), "port": int(db.get("port", 5432)),
        "database": db.get("name", "postgres"), "user": db.get("user", "postgres"),
        "password": db.get("password", ""), "sslmode": db.get("sslmode", "disable"),
        "schema": db.get("schema", "zcube"),
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--schema",    default=None)
    args = parser.parse_args()

    with open(_CONFIG_YAML) as fh:
        cfg = yaml.safe_load(fh) or {}
    pg     = get_pg(cfg)
    schema = args.schema or pg["schema"]

    conn = psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["database"],
        user=pg["user"], password=pg["password"], sslmode=pg["sslmode"],
    )

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        sql = f"SELECT id, name FROM {schema}.item"
        if not args.overwrite:
            sql += " WHERE image_url IS NULL"
        sql += " ORDER BY name"
        cur.execute(sql)
        items = cur.fetchall()

    print(f"Items to process: {len(items)}")
    if args.dry_run:
        print("DRY RUN - no writes\n")

    # Group by translated English core name, but only keep groups where
    # we have a confident NL→EN mapping (avoids querying Wikipedia for
    # packaging codes, abbreviations, etc.)
    groups: dict[str, list[int]] = defaultdict(list)
    is_translated: dict[str, bool] = {}

    for item in items:
        raw  = item["name"] or str(item["id"])
        core = extract_core(raw)
        en   = to_english(core)
        groups[en].append(item["id"])
        # Confident match: NL_TO_EN changed the value
        is_translated[en] = (en.upper() != core.upper())

    # Only attempt lookup for groups with a confident translation
    lookup_names = sorted(n for n in groups if is_translated[n])
    skip_count   = sum(len(groups[n]) for n in groups if not is_translated[n])

    print(f"Unique translated names to look up: {len(lookup_names)}")
    print(f"Items skipped (no translation):     {skip_count}\n")

    image_cache: dict[str, str | None] = {}
    found = 0; not_found = 0

    for i, en in enumerate(lookup_names, 1):
        ids = groups[en]
        print(f"  [{i:>4}/{len(lookup_names)}] {en:<40} ({len(ids):>5} items)  ", end="", flush=True)

        url = fetch_image(en)
        time.sleep(_DELAY)
        image_cache[en] = url

        if url:
            short = url.split("/")[-1][:55]
            print(f"OK  {short}")
            found += 1
            if not args.dry_run:
                with conn.cursor() as cur:
                    cur.execute(
                        f"UPDATE {schema}.item SET image_url = %s WHERE id = ANY(%s)",
                        (url, ids),
                    )
                conn.commit()
        else:
            print("--  not found")
            not_found += 1

    conn.close()

    total_updated = sum(len(groups[n]) for n in lookup_names if image_cache.get(n))
    print(f"\n{'='*60}")
    print(f"Unique names : {len(unique):>6}  (found: {found}, not found: {not_found})")
    print(f"Items updated: {total_updated:>6} / {len(items)}")
    if not args.dry_run and total_updated:
        print("\nRestart the API to reload the cache and see thumbnails in the dashboard.")


if __name__ == "__main__":
    main()
