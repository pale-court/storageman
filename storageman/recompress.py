import codecs
import hashlib
from itertools import zip_longest
import itertools
from pathlib import Path
import os
from typing import Set
import psycopg
from zstandard import ZstdCompressor


def main():
    db_uri = os.environ["INYA_DB_URI"]
    with psycopg.connect(db_uri) as conn:
        with conn.cursor() as cur:
            existing_sizes = {}
            cur.execute(
                "SELECT content_hash, length(data) FROM data WHERE compression is null"
            )
            for hash, byte_size in cur.fetchall():
                existing_sizes[hash] = byte_size
            print(f"{len(existing_sizes)}")

            cctx = ZstdCompressor()
            for hash, byte_size in existing_sizes.items():
                cur.execute(
                    "SELECT data FROM data WHERE content_hash = %s and compression is null",
                    (hash,),
                )
                if row := cur.fetchone():
                    cdata = cctx.compress(row[0])
                    if len(cdata) < byte_size:
                        print(f"{hash.hex()}: {len(cdata)} < {byte_size}")
                        cur.execute(
                            "UPDATE data SET data = %s, compression = 'zstd' WHERE content_hash = %s",
                            (cdata, hash),
                        )
                        conn.commit()
