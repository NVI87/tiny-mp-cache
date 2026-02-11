#!/usr/bin/env python3
import multiprocessing as mp
import os
import time
import json
from pathlib import Path

from tiny_mp_cache.tiny_mp_cache import serve_unix, TinyCache


# Куда кладём файлы (можешь поменять путь)
DATA_DIR = Path("./_snapshot_data_uds").absolute()


def uds_server(data_dir: str, sock_path: str):
    # snapshot_interval_secs=10, retention_chunks=15
    if os.path.exists(sock_path):
        os.remove(sock_path)
    serve_unix(data_dir, 10, 15)


def main():
    mp.set_start_method("fork", force=True)

    os.makedirs(DATA_DIR, exist_ok=True)
    ipc_dir = DATA_DIR / "ipc"
    os.makedirs(ipc_dir, exist_ok=True)

    sock_path = ipc_dir / "tiny-cache.sock"
    addr = f"unix://{sock_path}"
    print(f"DATA_DIR = {DATA_DIR}")
    print(f"UDS addr = {addr}")

    srv = mp.Process(target=uds_server, args=(str(DATA_DIR), str(sock_path)), daemon=True)
    srv.start()
    time.sleep(0.5)

    cache = TinyCache(addr)

    # Немного данных
    for i in range(200):
        cache.set(f"key:{i}", json.dumps({"i": i}).encode("utf-8"))
        time.sleep(1)
    cache.set("ttl:short", b"short", ttl=1.0)
    cache.set("ttl:long", b"long", ttl=10.0)

    print("Wrote keys over UDS, waiting for snapshots...")
    for sec in range(8, 0, -1):
        print(f"  wait {sec} s", end="\r", flush=True)
        time.sleep(1.0)
    print()

    print("\n=== Files in DATA_DIR (UDS) ===")
    for root, dirs, files in os.walk(DATA_DIR):
        rel_root = os.path.relpath(root, DATA_DIR)
        print(f"[{rel_root}]")
        for name in sorted(files):
            p = Path(root) / name
            print(f"  {name:20} {p.stat().st_size} bytes")

    print("\nget('ttl:short') ->", cache.get("ttl:short"))
    print("get('ttl:long')  ->", cache.get("ttl:long"))

    print("\nLeave process running to inspect files manually.")
    print("Press Ctrl+C to stop.")
    try:
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("\nStopping server...")
    finally:
        srv.terminate()
        srv.join(timeout=1)


if __name__ == "__main__":
    main()
