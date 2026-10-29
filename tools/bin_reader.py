#!/usr/bin/env python3
import sys
import bincode  # pip install bincode

from dataclasses import dataclass
from typing import List


@dataclass
class KeyMetaDisk:
    key: str
    key_id: int
    chunk_id: int
    ttl_ms: int
    updated_at_ms: int


@dataclass
class KeysFile:
    format_version: int
    entries: List[KeyMetaDisk]


def main(path: str):
    with open(path, "rb") as f:
        data = f.read()
    obj = bincode.loads(data)  # будет dict с теми же полями
    print("format_version:", obj["format_version"])
    for e in obj["entries"]:
        print(e)


if __name__ == "__main__":
    main(sys.argv[1])
