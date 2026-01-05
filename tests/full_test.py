#!/usr/bin/env python3
import multiprocessing as mp
import time
import json
from tiny_mp_cache import serve, serve_unix, TinyCache

TCP_PORT = 5002
TCP_ADDR = f"127.0.0.1:{TCP_PORT}"

UDS_PATH = "/tmp/tiny-mp-cache-test.sock"
UDS_ADDR = f"unix://{UDS_PATH}"

N_JOBS = 1000
N_WORKERS = 8
WORK_TIME = 0.001  # 1 мс на задачу


def tcp_server():
    serve(TCP_PORT)


def uds_server():
    serve_unix(UDS_PATH)


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
    отсутствия повторной обработки задач, не для бизнес-логики. [web:110]
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


def run_load_test(name: str, addr: str, server_target):
    print(f"\n=== {name} load test ===")
    mp.set_start_method("fork", force=True)

    srv = mp.Process(target=server_target, daemon=True)
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

        if len(unique_ids) != N_JOBS:
            missing = set(range(N_JOBS)) - unique_ids
            print(f"WARNING: missing {len(missing)} jobs, e.g. {sorted(list(missing))[:10]}")
        if duplicates:
            print("ERROR: some jobs were processed more than once!")
        else:
            print("OK: no duplicate processing detected")

    print(f"ALL DONE ({name})")


def main():
    # TCP
    run_load_test("TCP", TCP_ADDR, tcp_server)

    # UDS (только на Unix)
    try:
        run_load_test("UDS", UDS_ADDR, uds_server)
    except (AttributeError, OSError):
        print("\nUDS load test skipped (not supported on this platform)")


if __name__ == "__main__":
    main()
