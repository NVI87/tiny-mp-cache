#!/usr/bin/env python3
import multiprocessing as mp
import time
import json
from tiny_mp_cache import serve, TinyCache

PORT = 5002
ADDR = f"127.0.0.1:{PORT}"
N_JOBS = 1000
N_WORKERS = 8
WORK_TIME = 0.001  # 1 мс на задачу


def server():
    serve(PORT)


def make_job_key(i: int) -> str:
    return f"job:{i}"


def producer():
    cache = TinyCache(ADDR)
    for i in range(N_JOBS):
        job = {"id": i, "payload": f"data-{i}"}
        cache.set(make_job_key(i), json.dumps(job).encode("utf-8"))
    print(f"[PRODUCER] queued {N_JOBS} jobs")


def worker(worker_id: int, processed_ids):
    """
    processed_ids — общая manager.list(), куда пишем id обработанных задач.
    Используется только для проверки дубликатов, не для бизнес-логики. [web:110]
    """
    cache = TinyCache(ADDR)
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
            jid = job["id"]
            processed_ids.append(jid)
            processed += 1
            time.sleep(WORK_TIME)
    print(f"[WORKER {worker_id}] processed {processed} jobs")


def main():
    mp.set_start_method("fork", force=True)

    # стартуем сервер
    srv = mp.Process(target=server, daemon=True)
    srv.start()
    time.sleep(0.5)

    t0 = time.time()
    prod = mp.Process(target=producer)
    prod.start()
    prod.join()

    with mp.Manager() as manager:
        processed_ids = manager.list()

        workers = [
            mp.Process(target=worker, args=(wid, processed_ids))
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

    print("ALL DONE")


if __name__ == "__main__":
    main()
