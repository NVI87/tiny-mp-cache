#!/usr/bin/env python3
"""
Crash consistency: непрерывная запись во время snapshot'ов, затем SIGKILL сервера.

После рестарта должны найтись все записи, которые сервер успел подтвердить,
включая попавшие в окно между выгрузкой чанка и переключением WAL.
"""
import multiprocessing as mp
import os
import signal
import tempfile
import time

from tiny_mp_cache import serve, TinyCache


PORT = 5012
ADDR = f"tcp://127.0.0.1:{PORT}"

SNAPSHOT_INTERVAL_SECS = 1
RETENTION_CHUNKS = 100
WRITE_SECONDS = 3.5  # несколько snapshot'ов за время записи


def server(data_dir: str):
    serve(
        data_dir,
        PORT,
        snapshot_interval_secs=SNAPSHOT_INTERVAL_SECS,
        retention_chunks=RETENTION_CHUNKS,
    )


def start_server(data_dir: str) -> mp.Process:
    p = mp.Process(target=server, args=(data_dir,))
    p.start()
    time.sleep(0.5)
    return p


def value_for(i: int) -> bytes:
    return f"v-{i}".encode()


def main():
    mp.set_start_method("fork", force=True)

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-crash-") as data_dir:
        p1 = start_server(data_dir)
        c1 = TinyCache(ADDR)

        # часть ключей перезаписываем, часть удаляем — чтобы проверить не только set
        written = 0
        deadline = time.time() + WRITE_SECONDS
        while time.time() < deadline:
            c1.set(f"c:{written}", value_for(written))
            if written % 10 == 9:
                c1.set(f"c:{written - 5}", value_for(written - 5) + b"-upd")
                c1.delete(f"c:{written - 9}")
            written += 1

        # жёсткое падение без какого-либо завершения
        os.kill(p1.pid, signal.SIGKILL)
        p1.join(timeout=2)
        time.sleep(0.2)

        expected = {}
        for i in range(written):
            expected[f"c:{i}"] = value_for(i)
            if i % 10 == 9:
                expected[f"c:{i - 5}"] = value_for(i - 5) + b"-upd"
                expected.pop(f"c:{i - 9}", None)

        p2 = start_server(data_dir)
        c2 = TinyCache(ADDR)
        try:
            keys = set(c2.keys("c:*"))
            missing = sorted(set(expected) - keys, key=lambda k: int(k[2:]))
            extra = sorted(keys - set(expected), key=lambda k: int(k[2:]))
            print(f"written={written}, expected={len(expected)}, restored={len(keys)}")
            assert not missing, f"lost {len(missing)} acknowledged keys, e.g. {missing[:10]}"
            assert not extra, f"{len(extra)} deleted keys resurrected, e.g. {extra[:10]}"

            wrong = [k for k, v in expected.items() if c2.get(k) != v]
            assert not wrong, f"{len(wrong)} keys restored with wrong value, e.g. {wrong[:10]}"
            print("crash_consistency_test: OK")
        finally:
            p2.terminate()
            p2.join(timeout=2)


if __name__ == "__main__":
    main()
