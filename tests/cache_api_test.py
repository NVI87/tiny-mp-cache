#!/usr/bin/env python3
import multiprocessing as mp
import os
import tempfile
import time

from tiny_mp_cache.tiny_mp_cache import serve, serve_unix, TinyCache  # serve_unix доступен только на Unix


TCP_PORT = 5002
TCP_ADDR = f"tcp://127.0.0.1:{TCP_PORT}"

UDS_ADDR_TEMPLATE = "unix://{sock}"  # форматируем позже


def tcp_server(stop_event: mp.Event, data_dir: str):
    """
    TCP-сервер с супервизором: перезапускает serve() при крэше, пока не попросят остановиться.
    snapshot_interval_secs=2, retention_chunks=3 – достаточно часто для теста.
    """
    while not stop_event.is_set():
        try:
            # позиционные аргументы: data_dir, port, snapshot_interval_secs, retention_chunks
            serve(data_dir, TCP_PORT, 2, 3)
            # нормальное завершение — выходим из цикла
            break
        except Exception as e:
            print(f"[SUPERVISOR TCP] server crashed with {e!r}, restarting in 1s")
            if stop_event.is_set():
                break
            time.sleep(1.0)

    print("[SUPERVISOR TCP] loop stopped")


def uds_server(stop_event: mp.Event, data_dir: str, sock_path: str):
    """
    UDS-сервер с супервизором, аналогично TCP.
    """
    while not stop_event.is_set():
        try:
            if os.path.exists(sock_path):
                os.remove(sock_path)
            # позиционные: data_dir, snapshot_interval_secs, retention_chunks
            serve_unix(data_dir, 2, 3)
            break
        except Exception as e:
            print(f"[SUPERVISOR UDS] server crashed with {e!r}, restarting in 1s")
            if stop_event.is_set():
                break
            time.sleep(1.0)

    print("[SUPERVISOR UDS] loop stopped")


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

    print(f"== [{addr}] TTL ==")
    # короткий TTL
    c.set("test:ttl_short", b"v", ttl=0.5)
    # длинный TTL
    c.set("test:ttl_long", b"v", ttl=5.0)
    assert c.get("test:ttl_short") == b"v"
    assert c.get("test:ttl_long") == b"v"

    time.sleep(1.0)  # ждём, пока истечёт короткий TTL

    assert c.get("test:ttl_short") is None
    assert c.get("test:ttl_long") == b"v"

    ttl_keys = c.keys("test:ttl_*")
    print("ttl_keys:", ttl_keys)
    assert "test:ttl_short" not in ttl_keys
    assert "test:ttl_long" in ttl_keys

    print(f"ALL API TESTS PASSED for {addr}\n")


def main():
    mp.set_start_method("fork", force=True)

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-api-") as data_dir:
        # --- TCP ---
        tcp_stop_event = mp.Event()
        srv_tcp = mp.Process(
            target=tcp_server,
            args=(tcp_stop_event, data_dir),
            daemon=True,
        )
        srv_tcp.start()
        time.sleep(0.5)
        run_api_tests(TCP_ADDR)
        tcp_stop_event.set()
        srv_tcp.join(timeout=1)

        # --- UDS (только на Unix) ---
        try:
            uds_stop_event = mp.Event()
            sock_path = os.path.join(data_dir, "ipc", "tiny-cache.sock")
            addr = UDS_ADDR_TEMPLATE.format(sock=sock_path)
            srv_uds = mp.Process(
                target=uds_server,
                args=(uds_stop_event, data_dir, sock_path),
                daemon=True,
            )
            srv_uds.start()
            time.sleep(0.5)
            run_api_tests(addr)
            uds_stop_event.set()
            srv_uds.join(timeout=1)
        except (AttributeError, OSError):
            # на non-Unix serve_unix может не существовать / не работать
            print("UDS tests skipped (not supported on this platform)")


if __name__ == "__main__":
    main()
