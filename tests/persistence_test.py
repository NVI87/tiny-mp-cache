#!/usr/bin/env python3
import multiprocessing as mp
import os
import shutil
import tempfile
import time

from tiny_mp_cache import serve, TinyCache


PORT = 5003
ADDR = f"tcp://127.0.0.1:{PORT}"


def server(data_dir: str):
    serve(data_dir, PORT, snapshot_interval_secs=0, retention_chunks=3)


def start_server(data_dir: str):
    mp.set_start_method("fork", force=True)
    p = mp.Process(target=server, args=(data_dir,), daemon=True)
    p.start()
    time.sleep(0.5)
    return p


def main():
    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-persist-") as data_dir:
        # первый запуск: пишем данные
        p1 = start_server(data_dir)
        c1 = TinyCache(ADDR)

        c1.set("p:keep", b"v1")
        c1.set("p:delete", b"to-delete")
        c1.set("p:pop", b"to-pop")

        assert c1.get("p:keep") == b"v1"
        assert c1.get("p:delete") == b"to-delete"
        assert c1.get("p:pop") == b"to-pop"

        # операции, которые должны отразиться в WAL
        c1.delete("p:delete")
        v_pop = c1.pop("p:pop")
        assert v_pop == b"to-pop"
        assert c1.get("p:delete") is None
        assert c1.get("p:pop") is None

        p1.terminate()
        p1.join()

        # убеждаемся, что WAL/log.bin реально существует
        wal_path = os.path.join(data_dir, "wal", "log.bin")
        assert os.path.exists(wal_path), "WAL file must exist after first run"

        # второй запуск: восстановление из WAL + меты
        p2 = start_server(data_dir)
        c2 = TinyCache(ADDR)

        assert c2.get("p:keep") == b"v1"
        assert c2.get("p:delete") is None
        assert c2.get("p:pop") is None

        print("PERSISTENCE TEST PASSED")

        p2.terminate()
        p2.join()

    try:
        shutil.rmtree(data_dir, ignore_errors=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()
