#!/usr/bin/env python3
import multiprocessing as mp
import os
import time
import json
from pathlib import Path

from tiny_mp_cache.tiny_mp_cache import serve, TinyCache


TCP_PORT = 5003
TCP_ADDR = f"tcp://127.0.0.1:{TCP_PORT}"

# Куда кладём файлы (можешь поменять путь под себя)
DATA_DIR = Path("./_snapshot_data").absolute()


def tcp_server(data_dir: str):
    # Частые снапшоты, мало чанков, чтобы файлы обновлялись быстро
    serve(data_dir, TCP_PORT, 2, 3)


def main():
    mp.set_start_method("fork", force=True)

    os.makedirs(DATA_DIR, exist_ok=True)
    print(f"DATA_DIR = {DATA_DIR}")

    srv = mp.Process(target=tcp_server, args=(str(DATA_DIR),), daemon=True)
    srv.start()
    time.sleep(0.5)

    cache = TinyCache(TCP_ADDR)

    # Генерируем немного данных с TTL и без
    for i in range(20):
        cache.set(f"key:{i}", json.dumps({"i": i}).encode("utf-8"))
    cache.set("ttl:short", b"short", ttl=1.0)
    cache.set("ttl:long", b"long", ttl=10.0)

    print("Wrote keys, waiting for snapshots...")
    # ждём несколько циклов снапшота
    for sec in range(8, 0, -1):
        print(f"  wait {sec} s", end="\r", flush=True)
        time.sleep(1.0)
    print()

    print("\n=== Files in DATA_DIR ===")
    for root, dirs, files in os.walk(DATA_DIR):
        rel_root = os.path.relpath(root, DATA_DIR)
        print(f"[{rel_root}]")
        for name in sorted(files):
            p = Path(root) / name
            print(f"  {name:20} {p.stat().st_size} bytes")

    # Можно дополнительно проверить, что после снапшота TTL-ключи отработали
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
