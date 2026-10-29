#!/usr/bin/env python3
"""
Бенчмарк tiny-mp-cache (TCP, localhost).

Сценарии:
- seq_*: один клиент, последовательные запросы — задержка одного запроса;
- latency_get: p50/p99 задержки get;
- procs_N: N процессов, у каждого свой клиент, 90% get / 10% set — пропускная способность сервера;
- threads_N: N потоков одного процесса, у каждого свой клиент — проверка, что клиент не держит GIL.

Запуск:  python bench/bench.py [--quick] [--json results.json]
"""
import argparse
import importlib.metadata
import json
import multiprocessing as mp
import os
import platform
import random
import statistics
import tempfile
import threading
import time

from tiny_mp_cache import serve, TinyCache

PORT = 5030
ADDR = f"tcp://127.0.0.1:{PORT}"
N_KEYS = 10_000


def server(data_dir: str):
    serve(data_dir, PORT, snapshot_interval_secs=60, retention_secs=600)


def seq_ops(op: str, value: bytes, n: int) -> float:
    c = TinyCache(ADDR)
    keys = [f"k:{i % N_KEYS}" for i in range(n)]
    t0 = time.perf_counter()
    if op == "set":
        for k in keys:
            c.set(k, value)
    else:
        for k in keys:
            c.get(k)
    return n / (time.perf_counter() - t0)


def latency_get(n: int) -> dict:
    c = TinyCache(ADDR)
    samples = []
    for i in range(n):
        t0 = time.perf_counter_ns()
        c.get(f"k:{i % N_KEYS}")
        samples.append((time.perf_counter_ns() - t0) / 1000)
    samples.sort()
    return {
        "p50_us": round(samples[len(samples) // 2], 1),
        "p99_us": round(samples[int(len(samples) * 0.99)], 1),
        "mean_us": round(statistics.fmean(samples), 1),
    }


def mixed_worker(seconds: float, counter, seed: int):
    c = TinyCache(ADDR)
    rnd = random.Random(seed)
    value = b"x" * 64
    n = 0
    deadline = time.perf_counter() + seconds
    while time.perf_counter() < deadline:
        for _ in range(100):
            k = f"k:{rnd.randrange(N_KEYS)}"
            if rnd.random() < 0.1:
                c.set(k, value)
            else:
                c.get(k)
        n += 100
    with counter.get_lock():
        counter.value += n


def mixed_procs(n_procs: int, seconds: float) -> float:
    counter = mp.Value("q", 0)
    procs = [mp.Process(target=mixed_worker, args=(seconds, counter, i)) for i in range(n_procs)]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
    return counter.value / seconds


def mixed_threads(n_threads: int, seconds: float) -> float:
    class Counter:
        def __init__(self):
            self.value = 0
            self._lock = threading.Lock()

        def get_lock(self):
            return self._lock

    counter = Counter()
    threads = [
        threading.Thread(target=mixed_worker, args=(seconds, counter, i)) for i in range(n_threads)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return counter.value / seconds


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true", help="короткий прогон")
    ap.add_argument("--json", help="сохранить результаты в файл")
    args = ap.parse_args()

    n_seq = 20_000 if args.quick else 100_000
    seconds = 1.0 if args.quick else 3.0
    mp.set_start_method("fork", force=True)

    results = {
        "version": importlib.metadata.version("tiny-mp-cache"),
        "platform": f"{platform.system()} {platform.machine()}",
        "cpus": os.cpu_count(),
        "python": platform.python_version(),
    }

    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-bench-") as data_dir:
        srv = mp.Process(target=server, args=(data_dir,))
        srv.start()
        time.sleep(0.5)
        try:
            seq_ops("set", b"x" * 64, N_KEYS)  # прогрев и заполнение ключей
            results["seq_set_64B_ops"] = round(seq_ops("set", b"x" * 64, n_seq))
            results["seq_get_64B_ops"] = round(seq_ops("get", b"", n_seq))
            results["seq_set_4KB_ops"] = round(seq_ops("set", b"x" * 4096, n_seq // 2))
            seq_ops("set", b"x" * 64, N_KEYS)  # вернуть 64-байтные значения после 4 КБ
            results["latency_get"] = latency_get(n_seq)
            for n in (1, 4, 8, 16):
                results[f"procs_{n}_ops"] = round(mixed_procs(n, seconds))
            for n in (1, 8):
                results[f"threads_{n}_ops"] = round(mixed_threads(n, seconds))
        finally:
            srv.terminate()
            srv.join(timeout=5)

    width = max(len(k) for k in results)
    for k, v in results.items():
        print(f"{k:<{width}}  {v}")
    if args.json:
        with open(args.json, "w") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)


if __name__ == "__main__":
    main()
