#!/usr/bin/env python3
import multiprocessing as mp
import time
from tiny_mp_cache import serve, TinyCache

PORT = 5002
ADDR = f"127.0.0.1:{PORT}"


def server():
    serve(PORT)


def run_tests():
    c = TinyCache(ADDR)

    # очистим test:* на всякий случай
    for k in c.keys("test:*"):
        c.delete(k)

    print("== set/get ==")
    c.set("test:a", b"value-a")
    c.set("test:b", b"value-b")
    assert c.get("test:a") == b"value-a"
    assert c.get("test:b") == b"value-b"
    assert c.get("test:missing") is None

    print("== len ==")
    length = c.len()
    print("len after 2 keys:", length)
    assert length >= 2  # может быть больше из-за других тестов

    print("== keys(pattern) ==")
    keys = sorted(c.keys("test:*"))
    print("keys:", keys)
    assert "test:a" in keys and "test:b" in keys

    print("== delete ==")
    n1 = c.delete("test:a")
    n2 = c.delete("test:a")
    print("delete test:a ->", n1, n2)
    assert n1 == 1
    assert n2 == 0
    assert c.get("test:a") is None

    print("== pop ==")
    c.set("test:pop", b"payload")
    v1 = c.pop("test:pop")
    v2 = c.pop("test:pop")
    print("pop1:", v1, "pop2:", v2)
    assert v1 == b"payload"
    assert v2 is None

    print("\nALL API TESTS PASSED")


def main():
    mp.set_start_method("fork", force=True)

    srv = mp.Process(target=server, daemon=True)
    srv.start()
    time.sleep(0.5)

    run_tests()


if __name__ == "__main__":
    main()
