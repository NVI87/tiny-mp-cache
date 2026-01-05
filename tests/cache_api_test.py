#!/usr/bin/env python3
import multiprocessing as mp
import time
from tiny_mp_cache import serve, serve_unix, TinyCache  # serve_unix доступен только на Unix


TCP_PORT = 5002
TCP_ADDR = f"127.0.0.1:{TCP_PORT}"

UDS_PATH = "/tmp/tiny-mp-cache-test.sock"
UDS_ADDR = f"unix://{UDS_PATH}"


def tcp_server():
    serve(TCP_PORT)


def uds_server():
    serve_unix(UDS_PATH)


def run_api_tests(addr: str):
    c = TinyCache(addr)

    # очистим test:* на всякий случай
    for k in c.keys("test:*"):
        c.delete(k)

    print(f"== [{addr}] set/get ==")
    c.set("test:a", b"value-a")
    c.set("test:b", b"value-b")
    assert c.get("test:a") == b"value-a"
    assert c.get("test:b") == b"value-b"
    assert c.get("test:missing") is None

    print(f"== [{addr}] len ==")
    length = c.len()
    print("len after 2 keys:", length)
    assert length >= 2  # может быть больше из-за других тестов

    print(f"== [{addr}] keys(pattern) ==")
    keys = sorted(c.keys("test:*"))
    print("keys:", keys)
    assert "test:a" in keys and "test:b" in keys

    print(f"== [{addr}] delete ==")
    n1 = c.delete("test:a")
    n2 = c.delete("test:a")
    print("delete test:a ->", n1, n2)
    assert n1 == 1
    assert n2 == 0
    assert c.get("test:a") is None

    print(f"== [{addr}] pop ==")
    c.set("test:pop", b"payload")
    v1 = c.pop("test:pop")
    v2 = c.pop("test:pop")
    print("pop1:", v1, "pop2:", v2)
    assert v1 == b"payload"
    assert v2 is None

    print(f"ALL API TESTS PASSED for {addr}\n")


def main():
    mp.set_start_method("fork", force=True)

    # --- TCP ---
    srv_tcp = mp.Process(target=tcp_server, daemon=True)
    srv_tcp.start()
    time.sleep(0.5)
    run_api_tests(TCP_ADDR)

    # --- UDS (только на Unix) ---
    try:
        srv_uds = mp.Process(target=uds_server, daemon=True)
        srv_uds.start()
        time.sleep(0.5)
        run_api_tests(UDS_ADDR)
    except (AttributeError, OSError):
        # на non-Unix serve_unix может не существовать / не работать
        print("UDS tests skipped (not supported on this platform)")


if __name__ == "__main__":
    main()
