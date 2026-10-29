#!/usr/bin/env python3
"""
Нагрузочный сценарий «очередь задач»: продюсер кладёт задачи, воркеры
разбирают их через pop. Каждая задача должна быть обработана ровно один раз.
"""
import json
import multiprocessing as mp
import os
import tempfile
import time

import tiny_mp_cache
from tiny_mp_cache import serve, TinyCache

TCP_PORT = 5018
TCP_ADDR = f"tcp://127.0.0.1:{TCP_PORT}"

N_JOBS = 1000
N_WORKERS = 8
WORK_TIME = 0.001  # 1 мс на задачу


def tcp_server(data_dir: str):
    serve(data_dir, TCP_PORT)


def uds_server(data_dir: str):
    tiny_mp_cache.serve_unix(data_dir)


def uds_addr(data_dir: str) -> str:
    # serve_unix слушает сокет <data_dir>/ipc/tiny-cache.sock
    return "unix://" + os.path.join(data_dir, "ipc", "tiny-cache.sock")


def make_job_key(i: int) -> str:
    return f"job:{i}"


def producer(addr: str):
    cache = TinyCache(addr)
    for i in range(N_JOBS):
        job = {"id": i, "payload": f"data-{i}"}
        cache.set(make_job_key(i), json.dumps(job).encode("utf-8"))
    print(f"[PRODUCER {addr}] queued {N_JOBS} jobs")


def worker(worker_id: int, addr: str, processed_ids):
    """
    processed_ids — общая manager.list(), используется только для проверки
    отсутствия повторной обработки задач, не для бизнес-логики.
    """
    cache = TinyCache(addr)
    processed = 0
    while True:
        keys = cache.keys("job:*")
        if not keys:
            break
        for key in keys:
            raw = cache.pop(key)
            if raw is None:
                continue
            job = json.loads(raw.decode("utf-8"))
            processed_ids.append(job["id"])
            processed += 1
            time.sleep(WORK_TIME)
    print(f"[WORKER {worker_id} {addr}] processed {processed} jobs")


def run_load_test(name: str, addr: str, server_target, data_dir: str):
    print(f"\n=== {name} load test ===")

    srv = mp.Process(target=server_target, args=(data_dir,), daemon=True)
    srv.start()
    time.sleep(0.5)

    t0 = time.time()
    prod = mp.Process(target=producer, args=(addr,))
    prod.start()
    prod.join()

    with mp.Manager() as manager:
        processed_ids = manager.list()

        workers = [
            mp.Process(target=worker, args=(wid, addr, processed_ids))
            for wid in range(N_WORKERS)
        ]
        for p in workers:
            p.start()
        for p in workers:
            p.join()
        t1 = time.time()

        ids = list(processed_ids)
        unique_ids = set(ids)
        duplicates = len(ids) - len(unique_ids)

        print("\n=== STATS ===")
        print(f"transport  : {name}")
        print(f"jobs       : {N_JOBS}")
        print(f"workers    : {N_WORKERS}")
        print(f"work_time  : {WORK_TIME:.3f} s per job")
        print(f"elapsed    : {t1 - t0:.3f} s")
        print(f"processed  : total={len(ids)}, unique={len(unique_ids)}, duplicates={duplicates}")

        missing = set(range(N_JOBS)) - unique_ids
        assert not missing, f"missing {len(missing)} jobs, e.g. {sorted(missing)[:10]}"
        assert not duplicates, f"{duplicates} jobs were processed more than once"
        print("OK: every job processed exactly once")

    srv.terminate()
    srv.join(timeout=5)
    print(f"ALL DONE ({name})")


def main():
    mp.set_start_method("fork", force=True)

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-full-") as data_dir:
        run_load_test("TCP", TCP_ADDR, tcp_server, data_dir)

    # UDS (только на Unix)
    if hasattr(tiny_mp_cache, "serve_unix"):
        with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-full-") as data_dir:
            run_load_test("UDS", uds_addr(data_dir), uds_server, data_dir)
    else:
        print("\nUDS load test skipped (not supported on this platform)")


if __name__ == "__main__":
    main()
