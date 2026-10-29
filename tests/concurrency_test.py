#!/usr/bin/env python3
"""
Параллельная работа клиента и сервера:
- один TinyCache на несколько потоков и свой экземпляр на поток;
- клиент отпускает GIL во время сетевого обмена;
- параллельные set/get одного ключа из разных процессов: get никогда не видит None;
- set ключа с истёкшим TTL не теряется из-за параллельных чтений;
- TinyCache, унаследованный через fork, работает и в дочернем, и в родительском процессе.
"""
import multiprocessing as mp
import os
import socket
import tempfile
import threading
import time

from tiny_mp_cache import serve, TinyCache


PORT = 5020
ADDR = f"tcp://127.0.0.1:{PORT}"
THREADS = 8
OPS = 500
PROCS = 4
ROUNDS = 2000


def server(data_dir: str):
    serve(data_dir, PORT, snapshot_interval_secs=1, retention_secs=60)


def run_threads(make_client):
    errors = []

    def body(tid: int):
        c = make_client()
        try:
            for i in range(OPS):
                key, val = f"th:{tid}:{i}", f"{tid}-{i}".encode()
                c.set(key, val)
                got = c.get(key)
                if got != val:
                    errors.append((key, got))
        except Exception as e:  # noqa: BLE001
            errors.append(repr(e))

    threads = [threading.Thread(target=body, args=(t,)) for t in range(THREADS)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, f"thread errors: {errors[:5]}"


def gil_probe(counter):
    # сервер, который принимает соединение (backlog), но никогда не отвечает
    ls = socket.socket()
    ls.bind(("127.0.0.1", 0))
    ls.listen(1)
    c = TinyCache(f"tcp://127.0.0.1:{ls.getsockname()[1]}")

    def blocked():
        try:
            c.get("never")
        except Exception:  # noqa: BLE001
            pass

    threading.Thread(target=blocked, daemon=True).start()
    time.sleep(0.2)
    # если клиент держит GIL, сюда мы не попадём никогда
    n, t0 = 0, time.time()
    while time.time() - t0 < 0.2:
        n += 1
    counter.value = n
    os._exit(0)


def hot_writer(wid: int):
    c = TinyCache(ADDR)
    for i in range(ROUNDS):
        c.set("hot", f"{wid}-{i}".encode())


def hot_reader(_rid: int):
    c = TinyCache(ADDR)
    for i in range(ROUNDS):
        assert c.get("hot") is not None, f"get returned None for an existing key at round {i}"


def ttl_writer(wid: int):
    c = TinyCache(ADDR)
    key = f"ttl:{wid}"
    for i in range(ROUNDS // 4):
        c.set(key, b"old", ttl=0.001)
        time.sleep(0.002)
        val = f"new-{i}".encode()
        c.set(key, val)
        got = c.get(key)
        assert got == val, f"acknowledged set lost at round {i}: {got!r}"


def ttl_reader(_rid: int):
    c = TinyCache(ADDR)
    deadline = time.time() + 3
    while time.time() < deadline:
        for w in range(PROCS):
            c.get(f"ttl:{w}")
            c.keys("ttl:*")
            c.len()


def run_procs(targets):
    procs = [mp.Process(target=t, args=(i,)) for i, t in enumerate(targets)]
    for p in procs:
        p.start()
    for p in procs:
        p.join(timeout=60)
    codes = [p.exitcode for p in procs]
    assert all(c == 0 for c in codes), f"worker exit codes: {codes}"


def fork_child(c):
    c.set("f:child", b"from-child")
    assert c.get("f:child") == b"from-child"
    assert c.get("f:parent") == b"from-parent"


def main():
    mp.set_start_method("fork", force=True)

    # GIL: отдельный процесс со сторожевым таймаутом, чтобы зависание не вешало тест
    counter = mp.Value("q", 0)
    p = mp.Process(target=gil_probe, args=(counter,))
    p.start()
    p.join(timeout=5)
    if p.is_alive():
        p.kill()
        raise AssertionError("client holds the GIL while waiting for the server")
    assert counter.value > 0, "probe did not run"
    print(f"GIL released: main thread made {counter.value} iterations while a request was blocked")

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-conc-") as data_dir:
        srv = mp.Process(target=server, args=(data_dir,))
        srv.start()
        time.sleep(0.5)
        try:
            shared = TinyCache(ADDR)
            run_threads(lambda: shared)
            print("shared client across threads: OK")
            run_threads(lambda: TinyCache(ADDR))
            print("client per thread: OK")

            TinyCache(ADDR).set("hot", b"init")
            run_procs([hot_writer] * PROCS + [hot_reader] * PROCS)
            print("parallel set/get of one key: OK")

            run_procs([ttl_writer] * PROCS + [ttl_reader] * PROCS)
            print("set after expired TTL under concurrent reads: OK")

            inherited = TinyCache(ADDR)
            inherited.set("f:parent", b"from-parent")
            child = mp.Process(target=fork_child, args=(inherited,))
            child.start()
            child.join(timeout=10)
            assert child.exitcode == 0, f"child with inherited client failed: {child.exitcode}"
            assert inherited.get("f:child") == b"from-child"
            print("inherited client after fork: OK")

            print("concurrency_test: OK")
        finally:
            srv.terminate()
            srv.join(timeout=5)


if __name__ == "__main__":
    main()
