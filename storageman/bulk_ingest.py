import codecs
from itertools import zip_longest
import itertools
from pathlib import Path
import os
from typing import Set
import psycopg
from zstandard import ZstdCompressor

DATA_DIR = Path(os.environ.get("INYA_DATA_DIR", "/mnt/inya/data"))


def walk_data(existing_hashes: Set[str]):
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if file.endswith(".bin.zst"):
                file = Path(file)
                hash = codecs.decode(file.name.split(".", 1)[0], "hex")
                if hash not in existing_hashes:
                    data = (root / file).read_bytes()
                    scheme = "zstd"
                    yield (hash, data, scheme)
            else:
                print(root, file)


def enumerate_new_local_hashes(existing_hashes: Set[str]):
    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            is_compressed = file.endswith(".bin.zst")
            if is_compressed or file.endswith(".bin"):
                file = Path(file)
                hash = codecs.decode(file.name.split(".", 1)[0], "hex")
                if hash not in existing_hashes:

                    def make_file_loader(p: Path):
                        return lambda: p.read_bytes()

                    yield (hash, make_file_loader(root / file), is_compressed)
            else:
                print(root, file)


SIZE_LIM = 50 * 2**20
COUNT_LIM = 10000


def main():
    db_uri = os.environ["INYA_DB_URI"]
    with psycopg.connect(db_uri) as conn:
        with conn.cursor() as cur:
            existing_hashes = set()
            cur.execute("SELECT content_hash FROM data")
            for (hash,) in cur.fetchall():
                existing_hashes.add(hash)
            print(f"{len(existing_hashes)}")

            new_entries = {e[0]: e for e in enumerate_new_local_hashes(existing_hashes)}
            print(f"{len(new_entries)=}")

            # gen = walk_data(existing_hashes)
            gen = iter(new_entries.items())
            while True:
                cur.execute(
                    """
                    CREATE TEMPORARY TABLE IF NOT EXISTS new_data (
                        content_hash BYTEA PRIMARY KEY NOT NULL,
                        data BYTEA NOT NULL,
                        compression TEXT)"""
                )
                new_rows = False
                size_acc = 0
                count_acc = 0
                with cur.copy(
                    "COPY new_data (content_hash, data, compression) FROM STDIN WITH (FORMAT binary)"
                ) as copy:
                    copy.set_types(["bytea", "bytea", "text"])
                    for hash, entry in gen:
                        new_rows = True

                        # Prepare new row
                        data = entry[1]()
                        is_compressed = entry[2]

                        if not is_compressed:
                            zctx = ZstdCompressor()
                            cdata = zctx.compress(data)
                            if len(cdata) < len(data):
                                data = cdata
                                is_compressed = True

                        scheme = "zstd" if is_compressed else None
                        tup = (hash, data, scheme)
                        copy.write_row(tup)

                        # Contribute to batch limit counts
                        size_acc += len(tup[1])
                        count_acc += 1
                        if size_acc >= SIZE_LIM or count_acc >= COUNT_LIM:
                            print(f"Committing {size_acc=}, {count_acc=}")
                            break
                    if not new_rows:
                        break

                if new_rows:
                    cur.execute(
                        """
                                INSERT INTO data
                                (SELECT * FROM new_data)
                                ON CONFLICT DO NOTHING
                                """
                    )

                cur.execute("DROP TABLE new_data")

                conn.commit()

        # with conn.cursor() as cur:
        #     cur.execute("SELECT content_hash FROM data")
        #     for hash, in cur:
        #         print(f"{hash.hex()=}")
