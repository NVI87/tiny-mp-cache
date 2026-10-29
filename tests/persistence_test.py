#!/usr/bin/env python3
"""
Восстановление состояния после перезапуска сервера.

Проверяются оба пути восстановления:
- данные, попавшие в snapshot (после него WAL обнуляется) -> восстановление из chunks/*.bin;
- данные, записанные после snapshot'а -> доигрывание WAL.
"""
import multiprocessing as mp
import os
import tempfile
import time

from tiny_mp_cache import serve, TinyCache


PORT = 5008
ADDR = f"tcp://127.0.0.1:{PORT}"

SNAPSHOT_INTERVAL_SECS = 1
# большой retention, чтобы вытеснение по чанкам не влияло на этот тест
RETENTION_CHUNKS = 100


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


def stop_server(p: mp.Process):
    p.terminate()
    p.join(timeout=2)
    time.sleep(0.2)


def wait_for_snapshot(data_dir: str, timeout: float = 5.0):
    """Ждём, пока появится хотя бы один chunk-файл и WAL будет обнулён."""
    chunks_dir = os.path.join(data_dir, "chunks")
    wal_path = os.path.join(data_dir, "wal", "log.bin")
    deadline = time.time() + timeout
    while time.time() < deadline:
        chunks = [f for f in os.listdir(chunks_dir) if f.endswith(".bin")]
        if chunks and os.path.getsize(wal_path) == 0:
            return chunks
        time.sleep(0.1)
    raise AssertionError(f"snapshot was not taken within {timeout}s")


def main():
    mp.set_start_method("fork", force=True)

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-persist-") as data_dir:
        # --- первый запуск: данные, которые уйдут в snapshot ---
        p1 = start_server(data_dir)
        c1 = TinyCache(ADDR)

        c1.set("p:keep", b"v1")
        c1.set("p:delete", b"to-delete")
        c1.set("p:pop", b"to-pop")
        c1.delete("p:delete")
        assert c1.pop("p:pop") == b"to-pop"

        chunks = wait_for_snapshot(data_dir)
        print(f"snapshot taken: {chunks}")

        # --- данные после snapshot'а: живут только в WAL ---
        c1.set("p:after", b"v2")

        stop_server(p1)

        # --- второй запуск: проверяем восстановление ---
        p2 = start_server(data_dir)
        c2 = TinyCache(ADDR)

        try:
            got = {k: c2.get(k) for k in ("p:keep", "p:after", "p:delete", "p:pop")}
            print(f"after restart: {got}, len={c2.len()}")

            assert got["p:keep"] == b"v1", "value from snapshot was not restored"
            assert got["p:after"] == b"v2", "value from WAL was not restored"
            assert got["p:delete"] is None
            assert got["p:pop"] is None
            assert sorted(c2.keys("p:*")) == ["p:after", "p:keep"]
            assert c2.len() == 2
            print("persistence_test: OK")
        finally:
            stop_server(p2)


if __name__ == "__main__":
    main()
