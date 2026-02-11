#!/usr/bin/env python3
import multiprocessing as mp
import os
import random
import shutil
import tempfile
import time
from pathlib import Path

from tiny_mp_cache import serve, TinyCache


TCP_PORT = 5007
TCP_ADDR = f"tcp://127.0.0.1:{TCP_PORT}"

N_KEYS = 10_000
OPS_SETS = [1_000_000, 10_000_000]  # 100_000_000 слишком долго для CI, можно запускать вручную


def server(data_dir: str):
    # маленький snapshot_interval, чтобы часто обнулять WAL
    serve(data_dir, TCP_PORT, snapshot_interval_secs=30, retention_chunks=3)


def start_server(data_dir: str):
    mp.set_start_method("fork", force=True)
    p = mp.Process(target=server, args=(data_dir,), daemon=True)
    p.start()
    time.sleep(0.5)
    return p


def measure_persistence_size(data_dir: str) -> int:
    total = 0
    for root, _dirs, files in os.walk(data_dir):
        for f in files:
            if f.endswith(".bin") or f.endswith(".json"):
                total += os.path.getsize(os.path.join(root, f))
    return total


def run_one_experiment(total_ops: int):
    print(f"\n=== size asymptotics: total_ops={total_ops} ===")
    with tempfile.TemporaryDirectory(prefix="tiny-mp-cache-size-") as data_dir:
        p = start_server(data_dir)
        c = TinyCache(TCP_ADDR)

        keys = [f"k:{i}" for i in range(N_KEYS)]
        payload = b"x" * 100

        # начальная фаза: заполнение
        for k in keys:
            c.set(k, payload)

        ops_done = 0
        rnd = random.Random(0)

        while ops_done < total_ops:
            k = rnd.choice(keys)
            op = rnd.random()
            if op < 0.5:
                c.set(k, payload)
            elif op < 0.8:
                _ = c.get(k)
            elif op < 0.9:
                _ = c.delete(k)
            else:
                _ = c.pop(k)
            ops_done += 1

            if ops_done % 100_000 == 0:
                size_now = measure_persistence_size(data_dir)
                print(f"ops={ops_done}, size={size_now / 1024:.1f} KiB")

        # финальный размер
        final_size = measure_persistence_size(data_dir)
        print(f"FINAL size after {total_ops} ops: {final_size / 1024:.1f} KiB")

        p.terminate()
        p.join()

    try:
        shutil.rmtree(data_dir, ignore_errors=True)
    except Exception:
        pass


def main():
    for ops in OPS_SETS:
        run_one_experiment(ops)


if __name__ == "__main__":
    main()
