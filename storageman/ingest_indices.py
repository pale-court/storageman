import codecs
from itertools import zip_longest
import itertools
from pathlib import Path
import os
from typing import Set
import psycopg

DATA_DIR = Path(os.environ.get("INYA_DATA_DIR", "/mnt/inya/data"))
INDEX_DIR = Path(os.environ.get("INYA_INDEX_DIR", DATA_DIR.parent / "index"))


def enumerate_new_local_indices(existing_gids: Set[str]):
    for root, dirs, files in os.walk(INDEX_DIR):
        for file in files:
            if file.endswith(".ndjson.zst"):
                file = Path(file)
                base = file.name.split(".", 1)[0]
                parts = base.split("-")
                if len(parts) != 2:
                    print(root / file)
                    continue

                try:
                    gid = int(parts[0])
                except ValueError:
                    continue

                key = (parts[0], parts[1])
                if key not in existing_gids:

                    def make_file_loader(p: Path):
                        return lambda: p.read_bytes()

                    yield (key, make_file_loader(root / file))


def main():
    db_uri = os.environ["INYA_DB_URI"]
    with psycopg.connect(db_uri) as conn:
        with conn.cursor() as cur:
            existing_gids = set()
            cur.execute("SELECT gid, kind FROM index")
            for gid, kind in cur.fetchall():
                existing_gids.add((gid, kind))
            print(f"{len(existing_gids)}")

            new_entries = {e[0]: e for e in enumerate_new_local_indices(existing_gids)}
            print(f"{len(new_entries)=}")

            n = len(new_entries)
            for i, (key, loader) in enumerate(new_entries.values()):
                print(f"inserting {i+1}/{n}: {key}")
                cur.execute(
                    "INSERT INTO index (gid, kind, data, compression) VALUES (%s, %s, %s, 'zstd')",
                    (key[0], key[1], loader()),
                )
                conn.commit()
