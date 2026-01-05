#!/usr/bin/env python3
import multiprocessing as mp
import os
import time
from tiny_mp_cache import serve, TinyCache

PORT = 5003
ADDR = f"127.0.0.1:{PORT}"
WAL_FILE = "tiny-mp-cache.wal"  # должен совпадать с путём в serve()


def server():
    serve(PORT)


def start_server():
    mp.set_start_method("fork", force=True)
    p = mp.Process(target=server, daemon=True)
    p.start()
    time.sleep(0.5)
    return p


def main():
    # чистый старт
    if os.path.exists(WAL_FILE):
        os.remove(WAL_FILE)

    # первый запуск: пишем данные
    p1 = start_server()
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

    # завершаем первый сервер
    p1.terminate()
    p1.join()

    assert os.path.exists(WAL_FILE), "WAL file must exist after first run"

    # второй запуск: восстановление из WAL
    p2 = start_server()
    c2 = TinyCache(ADDR)

    # проверяем, что состояние восстановилось
    assert c2.get("p:keep") == b"v1"
    assert c2.get("p:delete") is None
    assert c2.get("p:pop") is None

    print("PERSISTENCE TEST PASSED")

    p2.terminate()
    p2.join()


if __name__ == "__main__":
    main()
