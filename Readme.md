# tiny-mp-cache

`tiny-mp-cache` — минималистичный k/v‑кэш на Rust с Python‑клиентом.  
Он рассчитан на использование как локальный сервис кэширования между несколькими Python‑процессами (например, воркерами `multiprocessing`) в одном pod’е или на одной машине.

Основные цели:

- надёжное и предсказуемое поведение;
- простой и чёткий API;
- возможность поднять сервер в отдельном процессе одной строкой из Python;
- производительность уровня Rust.

---

## Установка

Проект собирается через `maturin` и `PyO3` (Rust‑расширение для Python).

```bash
python -m venv .venv
source .venv/bin/activate  # или .venv\Scripts\activate на Windows

pip install maturin

# установка расширения в текущий venv (editable-режим)
maturin develop --release
```
После этого модуль tiny_mp_cache доступен в Python в этом виртуальном окружении.

При изменении Rust‑кода библиотеку нужно пересобирать повторным запуском maturin develop или maturin develop --release.

Быстрый старт
Запуск сервера из Python
```python
import multiprocessing as mp
import time
from tiny_mp_cache import serve, TinyCache

PORT = 5002
ADDR = f"127.0.0.1:{PORT}"


def server():
    # Rust-сервер, слушает 127.0.0.1:PORT и обрабатывает команды
    serve(PORT)


def main():
    mp.set_start_method("fork", force=True)  # для Linux/macOS

    # стартуем сервер в отдельном процессе
    srv = mp.Process(target=server, daemon=True)
    srv.start()
    time.sleep(0.5)  # небольшая пауза на инициализацию

    cache = TinyCache(ADDR)
    cache.set("foo", b"bar")
    value = cache.get("foo")
    print(value)  # b'bar'


if __name__ == "__main__":
    main()

```
serve(port) поднимает один TCP‑сервер на 127.0.0.1:port.

TinyCache — клиентский класс, который ходит к этому серверу по TCP.

API Python‑клиента
```python
from tiny_mp_cache import TinyCache

cache = TinyCache("127.0.0.1:5002")

```

Ключи — строки (str).
Значения — байты (bytes). Сериализацию/десериализацию объектов (JSON, pickle и т.п.) контролирует приложение.

set(key: str, value: bytes) -> None
Сохраняет значение по ключу.

```python
cache.set("user:1", b"payload")
get(key: str) -> Optional[bytes]
```
Возвращает значение по ключу или None, если ключа нет.

```python
value = cache.get("user:1")
if value is not None:
    print(value.decode("utf-8"))
pop(key: str) -> Optional[bytes]
```
Атомарно забирает значение и удаляет ключ.

Гарантия: если несколько воркеров одновременно вызывают pop для одного и того же ключа, значение получит ровно один из них.

```python
job_raw = cache.pop("job:123")
if job_raw is not None:
    job = job_raw.decode("utf-8")
delete(key: str) -> int
```
Удаляет ключ.

1 — ключ существовал и был удалён;

0 — ключа не было.

```python
deleted = cache.delete("user:1")
keys(pattern: str) -> list[str]
```
Возвращает список ключей, подходящих под паттерн.
Сейчас поддерживается только префиксный паттерн вида "prefix*".

```python
jobs = cache.keys("job:*")
len() -> int
```
Возвращает количество ключей в кэше.

```python
print(cache.len())
```
Пример: продюсер и воркеры
Пример использования кэша как простой очереди задач между несколькими процессами.

```python
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
    processed_ids — общая manager.list(), используется только для проверки,
    что не было повторной обработки задач.
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
            processed_ids.append(job["id"])
            processed += 1
            time.sleep(WORK_TIME)
    print(f"[WORKER {worker_id}] processed {processed} jobs")


def main():
    mp.set_start_method("fork", force=True)

    srv = mp.Process(target=server, daemon=True)
    srv.start()
    time.sleep(0.5)

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

        ids = list(processed_ids)
        unique_ids = set(ids)
        duplicates = len(ids) - len(unique_ids)

        print("\n=== STATS ===")
        print(f"jobs       : {N_JOBS}")
        print(f"workers    : {N_WORKERS}")
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
```
Запуск тестов
В репозитории есть два тестовых скрипта:

tests/cache_api_test.py — проверяет базовый API (set/get/pop/delete/keys/len) в одном процессе;

tests/full_test.py — нагрузочный многопроцессный сценарий с продюсером и воркерами.

Перед запуском убедись, что активирован тот же venv, куда ставился пакет через maturin develop.

```bash
python tests/cache_api_test.py
python tests/full_test.py
```