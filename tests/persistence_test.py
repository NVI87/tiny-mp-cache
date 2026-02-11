#!/usr/bin/env python3
import multiprocessing as mp
import os
import shutil
import tempfile
import time

from tiny_mp_cache import serve, TinyCache


PORT = 5008
ADDR = f"tcp://127.0.0.1:{PORT}"


def server(data_dir: str):
    # маленький, но не нулевой интервал, чтобы снапшот успевал сохраняться
    serve(data_dir, PORT, snapshot_interval_secs=10, retention_chunks=3)


def start_server(data_dir: str) -> mp.Process:
    mp.set_start_method("fork", force=True)
    p = mp.Process(target=server, args=(data_dir,))
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

        # даём серверу время сделать хотя бы один снапшот
        time.sleep(2.5)

        # останавливаем первый сервер
        p1.terminate()
        p1.join(timeout=2)
        time.sleep(0.2)

        # второй запуск: проверяем, что состояние восстановилось корректно
        p2 = start_server(data_dir)
        c2 = TinyCache(ADDR)

        try:
            print(f'c2.get("p:keep"): {c2.get("p:keep")}')
            print(f'c2.get("p:delete"): {c2.get("p:delete")}')
            print(f'c2.get("p:pop"): {c2.get("p:pop")}')

            assert c2.get("p:keep") == b"v1"
            assert c2.get("p:delete") is None
            assert c2.get("p:pop") is None
            print("persistence_test: OK")
        finally:
            p2.terminate()
            p2.join(timeout=2)
            time.sleep(0.2)

    try:
        shutil.rmtree(data_dir, ignore_errors=True)
    except Exception:
        pass


if __name__ == "__main__":
    main()
