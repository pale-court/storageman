import codecs
import hashlib
from itertools import zip_longest
import itertools
from pathlib import Path
import os
from typing import Set
import psycopg
from zstandard import ZstdCompressor

DATA_DIR = Path(os.environ.get("INYA_DATA_DIR", "/mnt/inya/data"))


def main():
    db_uri = os.environ["INYA_DB_URI"]
    with psycopg.connect(db_uri) as conn:
        with conn.cursor() as cur:
            todo = {}
            cur.execute(
                """
                SELECT content_hash, comp_hash.data_hash, dagur_hash.file_hash
                FROM comp_hash NATURAL LEFT OUTER JOIN dagur_hash
                WHERE comp_hash.data_hash IS DISTINCT FROM dagur_hash.file_hash
                """
            )
            for addr, comp, dagur in cur.fetchall():
                todo[addr] = (addr, comp, dagur)
            print(len(todo))

            count_acc = 0
            size_acc = 0
            COUNT_LIMIT = 1000
            SIZE_LIMIT = 50 * 2**20
            had_updates = False
            for addr, entry in todo.items():
                addr_text = addr.hex()
                p = DATA_DIR / addr_text[:2] / f"{addr_text}.bin.zst"
                data = p.read_bytes()
                remote_hash = entry[1]
                our_hash = entry[2]
                this_updated = False
                if our_hash is None:
                    # Local hash unknown, compute it
                    our_hash = hashlib.sha256(data, usedforsecurity=False).digest()
                    cur.execute(
                        "INSERT INTO dagur_hash VALUES (%s, %s)",
                        (addr, our_hash),
                    )
                    had_updates = True
                    this_updated = True
                if remote_hash != our_hash:
                    cur.execute(
                        "UPDATE data SET data = %s WHERE content_hash = %s",
                        (data, addr),
                    )
                    cur.execute(
                        "UPDATE comp_hash SET data_hash = %s WHERE content_hash = %s",
                        (our_hash, addr),
                    )
                    size_acc += len(data)
                    had_updates = True
                    this_updated = True

                if this_updated:
                    count_acc += 1

                if count_acc >= COUNT_LIMIT or size_acc >= SIZE_LIMIT:
                    print(f"commit {count_acc=}, {size_acc=}")
                    conn.commit()
                    count_acc = 0
                    size_acc = 0
                    had_updates = False

            if had_updates:
                conn.commit()
