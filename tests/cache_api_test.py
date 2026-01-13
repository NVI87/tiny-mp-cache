#!/usr/bin/env python3
import multiprocessing as mp
import os
import time
from tiny_mp_cache import serve, serve_unix, TinyCache  # serve_unix доступен только на Unix


TCP_PORT = 5002
TCP_ADDR = f"127.0.0.1:{TCP_PORT}"

UDS_PATH = "/tmp/tiny-mp-cache-test.sock"
UDS_ADDR = f"unix://{UDS_PATH}"

def uds_server(stop_event: mp.Event, wal_path: str | None = None):
    while not stop_event.is_set():
        try:
            if os.path.exists(UDS_PATH):
                os.remove(UDS_PATH)
            serve_unix(UDS_PATH)
            # нормальное завершение — выходим из цикла
            break
        except Exception as e:
            print(f"[SUPERVISOR] server crashed with {e!r}, restarting in 1s")
            # если уже попросили остановиться — не рестартуём
            if stop_event.is_set():
                break
            time.sleep(1.0)

    print("[SUPERVISOR] loop stopped")

def tcp_server(stop_event: mp.Event, wal_path: str | None = None):
    while not stop_event.is_set():
        try:
            serve(TCP_PORT)
            # нормальное завершение — выходим из цикла
            break
        except Exception as e:
            print(f"[SUPERVISOR] server crashed with {e!r}, restarting in 1s")
            # если уже попросили остановиться — не рестартуём
            if stop_event.is_set():
                break
            time.sleep(1.0)

    print("[SUPERVISOR] loop stopped")

# def tcp_server():
#     serve(TCP_PORT)
#
#
# def uds_server():
#     serve_unix(UDS_PATH)


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

    wal_dir = os.getcwd()
    # --- TCP ---
    tcp_stop_event = mp.Event()
    srv_tcp = mp.Process(target=tcp_server, args=(tcp_stop_event, wal_dir), daemon=True)
    srv_tcp.start()
    time.sleep(0.5)
    run_api_tests(TCP_ADDR)
    tcp_stop_event.set()
    srv_tcp.join(timeout=1)

    # --- UDS (только на Unix) ---
    try:
        uds_stop_event = mp.Event()
        srv_uds = mp.Process(target=uds_server, args=(uds_stop_event, wal_dir), daemon=True)
        srv_uds.start()
        time.sleep(0.5)
        run_api_tests(UDS_ADDR)
        uds_stop_event.set()
        srv_uds.join(timeout=1)
    except (AttributeError, OSError):
        # на non-Unix serve_unix может не существовать / не работать
        print("UDS tests skipped (not supported on this platform)")


if __name__ == "__main__":
    main()
