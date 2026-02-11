#!/usr/bin/env python3
import sys
import struct


def dump_keys_bin(path: str):
    with open(path, "rb") as f:
        data = f.read()

    off = 0

    # u32 format_version
    (format_version,) = struct.unpack_from("<I", data, off); off += 4

    # u64 entries_len (Vec length в bincode)
    (entries_len,) = struct.unpack_from("<Q", data, off); off += 8

    print("format_version:", format_version)
    print("entries_len  :", entries_len)
    print()

    for i in range(entries_len):
        # String key: u64 len + bytes
        (key_len,) = struct.unpack_from("<Q", data, off); off += 8
        key_bytes = data[off:off+key_len]; off += key_len
        key = key_bytes.decode("utf-8", errors="replace")

        # key_id: u64, chunk_id: u64, ttl_ms: i64, updated_at_ms: i64
        key_id, chunk_id, ttl_ms, updated_at_ms = struct.unpack_from("<QQqq", data, off)
        off += 8 + 8 + 8 + 8

        print(
            f"{i}: key={key!r}, key_id={key_id}, "
            f"chunk_id={chunk_id}, ttl_ms={ttl_ms}, updated_at_ms={updated_at_ms}"
        )


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("usage: dump_keys_bin.py path/to/keys.bin")
        sys.exit(1)
    dump_keys_bin(sys.argv[1])
