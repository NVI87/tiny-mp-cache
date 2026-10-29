#!/usr/bin/env python3
"""
Корректное завершение: SIGTERM -> финальный snapshot -> выход с кодом 0.

Интервал snapshot'а большой, поэтому без финального snapshot'а данные
остались бы только в WAL. После SIGTERM все сегменты WAL должны быть
пустыми (свёрнуты в chunk-файл), а данные — восстанавливаться при рестарте.
"""
import multiprocessing as mp
import os
import tempfile
import time

from tiny_mp_cache import serve, TinyCache


PORT = 5016
ADDR = f"tcp://127.0.0.1:{PORT}"


def server(data_dir: str):
    serve(data_dir, PORT, snapshot_interval_secs=3600, retention_secs=3600)


def start_server(data_dir: str) -> mp.Process:
    p = mp.Process(target=server, args=(data_dir,))
    p.start()
    time.sleep(0.5)
    return p


def wal_bytes(data_dir: str) -> int:
    d = os.path.join(data_dir, "wal")
    return sum(os.path.getsize(os.path.join(d, f)) for f in os.listdir(d))


def main():
    mp.set_start_method("fork", force=True)

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-shutdown-") as data_dir:
        p1 = start_server(data_dir)
        c1 = TinyCache(ADDR)
        for i in range(100):
            c1.set(f"g:{i}", f"v-{i}".encode())
        c1.delete("g:0")
        assert wal_bytes(data_dir) > 0, "writes must be in WAL before shutdown"

        p1.terminate()  # SIGTERM
        p1.join(timeout=5)
        print(f"exitcode={p1.exitcode}, wal bytes after shutdown={wal_bytes(data_dir)}")
        assert p1.exitcode == 0, f"server must exit cleanly on SIGTERM, got {p1.exitcode}"
        assert wal_bytes(data_dir) == 0, "final snapshot must fold WAL into chunks"

        p2 = start_server(data_dir)
        c2 = TinyCache(ADDR)
        try:
            assert c2.get("g:0") is None
            assert all(c2.get(f"g:{i}") == f"v-{i}".encode() for i in range(1, 100))
            assert c2.len() == 99
            print("graceful_shutdown_test: OK")
        finally:
            p2.terminate()
            p2.join(timeout=5)


if __name__ == "__main__":
    main()
