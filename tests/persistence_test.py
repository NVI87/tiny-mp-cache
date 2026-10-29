#!/usr/bin/env python3
"""
Восстановление состояния после перезапуска сервера.

Проверяется:
- данные, попавшие в snapshot (после него WAL обнуляется) -> восстановление из chunks/*.bin;
- ключ, записанный между snapshot'ами, переживает следующий snapshot;
- данные, записанные после snapshot'а -> доигрывание WAL;
- новые ключи после рестарта не затирают восстановленные.
"""
import json
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


def current_chunk_id(data_dir: str) -> int:
    with open(os.path.join(data_dir, "meta", "state.json")) as f:
        return json.load(f)["current_chunk_id"]


def wait_for_snapshots(data_dir: str, n: int = 2, timeout: float = 10.0):
    """
    Ждём n смен current_chunk_id: значит, хотя бы один snapshot целиком
    прошёл после момента вызова (первая смена могла быть у уже идущего snapshot'а).
    """
    seen = current_chunk_id(data_dir)
    changes = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        cur = current_chunk_id(data_dir)
        if cur != seen:
            seen = cur
            changes += 1
            if changes >= n:
                return
        time.sleep(0.05)
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

        # первый snapshot: уходят p:keep и т.д., WAL обнуляется
        wait_for_snapshots(data_dir)

        # ключ, записанный между snapshot'ами, должен пережить следующий snapshot
        c1.set("p:mid", b"v-mid")
        wait_for_snapshots(data_dir)
        assert c1.get("p:mid") == b"v-mid", "key written after a snapshot was lost by the next one"

        # --- данные после последнего snapshot'а: живут только в WAL ---
        c1.set("p:after", b"v2")

        stop_server(p1)

        # --- второй запуск: проверяем восстановление ---
        p2 = start_server(data_dir)
        c2 = TinyCache(ADDR)

        try:
            got = {k: c2.get(k) for k in ("p:keep", "p:mid", "p:after", "p:delete", "p:pop")}
            print(f"after restart: {got}, len={c2.len()}")

            assert got["p:keep"] == b"v1", "value from snapshot was not restored"
            assert got["p:mid"] == b"v-mid", "value from second snapshot was not restored"
            assert got["p:after"] == b"v2", "value from WAL was not restored"
            assert got["p:delete"] is None
            assert got["p:pop"] is None
            assert sorted(c2.keys("p:*")) == ["p:after", "p:keep", "p:mid"]
            assert c2.len() == 3

            # новые ключи после рестарта не должны затирать восстановленные (key_id)
            c2.set("p:new", b"v-new")
            assert c2.get("p:new") == b"v-new"
            assert c2.get("p:keep") == b"v1", "new key overwrote a restored one"
            assert c2.get("p:mid") == b"v-mid", "new key overwrote a restored one"
            print("persistence_test: OK")
        finally:
            stop_server(p2)


if __name__ == "__main__":
    main()
