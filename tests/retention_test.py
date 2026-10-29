#!/usr/bin/env python3
"""
Retention (конфиг сервера) не путается с TTL (параметр записи ключа):
- чанки старше retention_secs удаляются с диска, но их живые ключи
  переносятся в свежий snapshot и не пропадают — ни в памяти, ни после SIGKILL;
- ключ с TTL пропадает по своему TTL;
- число chunk-файлов на диске ограничено окном retention;
- retention_secs < snapshot_interval_secs отвергается.
"""
import multiprocessing as mp
import os
import signal
import tempfile
import time

from tiny_mp_cache import serve, TinyCache


PORT = 5014
ADDR = f"tcp://127.0.0.1:{PORT}"

SNAPSHOT_INTERVAL_SECS = 1
RETENTION_SECS = 2
# чанки окна retention + сохраняемый сейчас + запас на границе интервала
MAX_CHUNK_FILES = RETENTION_SECS // SNAPSHOT_INTERVAL_SECS + 2


def server(data_dir: str):
    serve(
        data_dir,
        PORT,
        snapshot_interval_secs=SNAPSHOT_INTERVAL_SECS,
        retention_secs=RETENTION_SECS,
    )


def start_server(data_dir: str) -> mp.Process:
    p = mp.Process(target=server, args=(data_dir,))
    p.start()
    time.sleep(0.5)
    return p


def chunk_files(data_dir: str) -> list:
    d = os.path.join(data_dir, "chunks")
    return sorted(f for f in os.listdir(d) if f.startswith("chunk_") and f.endswith(".bin"))


def check_alive(c: TinyCache, data_dir: str, stage: str):
    assert c.get("r:old") == b"old", f"[{stage}] untouched key lost after its chunk expired"
    assert c.get("r:ttl") is None, f"[{stage}] key outlived its TTL"
    files = chunk_files(data_dir)
    print(f"[{stage}] chunk files: {len(files)}")
    assert len(files) <= MAX_CHUNK_FILES, f"[{stage}] old chunks not removed: {files}"


def main():
    mp.set_start_method("fork", force=True)

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-retention-") as data_dir:
        # неверный конфиг отвергается сразу, до старта сервера
        try:
            serve(data_dir, PORT, snapshot_interval_secs=2, retention_secs=1)
        except ValueError as e:
            print(f"bad config rejected: {e}")
        else:
            raise AssertionError("retention_secs < snapshot_interval_secs must be rejected")

        p1 = start_server(data_dir)
        c1 = TinyCache(ADDR)

        c1.set("r:old", b"old")              # больше не трогаем
        c1.set("r:ttl", b"short", ttl=1.5)   # должен пропасть по TTL

        # несколько интервалов дольше retention: чанк с r:old давно устарел
        for _ in range(5):
            c1.set("r:tick", b"x")  # фоновые записи, чтобы чанки не были пустыми
            time.sleep(1.0)
        check_alive(c1, data_dir, "running")

        os.kill(p1.pid, signal.SIGKILL)
        p1.join(timeout=2)
        time.sleep(0.2)

        # после падения r:old восстанавливается из чанка, куда его перенесли
        p2 = start_server(data_dir)
        c2 = TinyCache(ADDR)
        try:
            assert c2.get("r:old") == b"old", "carried key not restored after SIGKILL"

            # и продолжает переноситься после рестарта
            time.sleep(RETENTION_SECS + 2.5)
            check_alive(c2, data_dir, "after restart")
            print("retention_test: OK")
        finally:
            p2.terminate()
            p2.join(timeout=2)


if __name__ == "__main__":
    main()
