# tiny-mp-cache

`tiny-mp-cache` — минималистичный персистентный k/v‑кэш на Rust с Python‑клиентом.  
Он рассчитан на использование как локальный сервис кэширования между несколькими Python‑процессами (например, воркерами `multiprocessing`) в одном pod’е или на одной машине.

- два транспорта: TCP (`serve`) и Unix domain socket (`serve_unix`, только Linux/macOS);
- TTL на уровне ключа;
- персистентность: WAL + периодические snapshot'ы в `data_dir`, восстановление после падения;
- атомарный `pop`: при конкурентных вызовах значение получает ровно один процесс.

На Windows доступен только TCP: функции `serve_unix` там нет, а `from tiny_mp_cache import serve_unix` вызовет `ImportError`.

> **0.2 несовместима с 0.1 по API сервера и формату данных** — см. [Миграция с 0.1](#миграция-с-01).

***

## Установка из PyPI

```bash
python -m venv .venv
source .venv/bin/activate  # или .venv\Scripts\activate на Windows

pip install tiny-mp-cache
```

***

## Локальная сборка из исходников

Проект использует `maturin` и `PyO3` (Rust‑расширение для Python).

```bash
python -m venv .venv
source .venv/bin/activate  # или .venv\Scripts\activate на Windows

pip install maturin

# установка расширения в текущий venv (editable-режим)
maturin develop --release
```

При изменении Rust‑кода библиотеку нужно пересобирать повторным запуском `maturin develop --release`.

***

## Быстрый старт: TCP‑сервер

```python
import multiprocessing as mp
import time
from tiny_mp_cache import serve, TinyCache

DATA_DIR = "/var/lib/tiny-mp-cache"
PORT = 5002
ADDR = f"tcp://127.0.0.1:{PORT}"


def server():
    # Rust-сервер на 127.0.0.1:PORT, данные и журнал — в DATA_DIR
    serve(DATA_DIR, PORT)


def main():
    mp.set_start_method("fork", force=True)  # для Linux/macOS

    srv = mp.Process(target=server)
    srv.start()
    time.sleep(0.5)  # пауза на инициализацию и восстановление

    cache = TinyCache(ADDR)
    cache.set("foo", b"bar")
    cache.set("session:1", b"token", ttl=30.0)  # ключ живёт 30 секунд
    print(cache.get("foo"))  # b'bar'

    srv.terminate()  # SIGTERM: финальный snapshot и выход с кодом 0
    srv.join()


if __name__ == "__main__":
    main()
```

***

## Быстрый старт: Unix domain socket

`serve_unix` слушает сокет `<data_dir>/ipc/tiny-cache.sock` (старый сокет при старте удаляется).

```python
import multiprocessing as mp
import os
import time
from tiny_mp_cache import serve_unix, TinyCache

DATA_DIR = "/var/lib/tiny-mp-cache"
UDS_ADDR = "unix://" + os.path.join(DATA_DIR, "ipc", "tiny-cache.sock")


def server():
    serve_unix(DATA_DIR)


def main():
    mp.set_start_method("fork", force=True)

    srv = mp.Process(target=server)
    srv.start()
    time.sleep(0.5)

    cache = TinyCache(UDS_ADDR)
    cache.set("foo", b"bar")
    print(cache.get("foo"))  # b'bar'

    srv.terminate()
    srv.join()


if __name__ == "__main__":
    main()
```

Длина пути к сокету ограничена ОС (около 104 байт на macOS и 108 на Linux), поэтому для `serve_unix` лучше выбирать короткий `data_dir`.

***

## API сервера

```python
serve(data_dir, port=5002, snapshot_interval_secs=60, retention_secs=600)
serve_unix(data_dir, snapshot_interval_secs=60, retention_secs=600)  # только Unix
```

- `data_dir` — каталог с данными (создаётся автоматически);
- `port` — TCP‑порт, сервер слушает только `127.0.0.1`;
- `snapshot_interval_secs` — интервал snapshot'ов (> 0);
- `retention_secs` — сколько секунд chunk‑файл snapshot'а хранится на диске (≥ `snapshot_interval_secs`).

Неверная конфигурация сразу вызывает `ValueError`.

`serve` блокирует вызвавший поток до сигнала, но GIL отпускает: остальные Python‑потоки процесса продолжают работать. По сигналу (Ctrl+C, SIGTERM) сервер делает финальный snapshot и пробрасывает исключение сигнала в Python. Если у процесса нет своего обработчика SIGTERM, на время работы `serve` ставится обработчик, который завершает процесс через `SystemExit(0)`, поэтому `Process.terminate()` даёт чистый выход с кодом 0.

***

## API Python‑клиента

```python
from tiny_mp_cache import TinyCache

cache_tcp = TinyCache("tcp://127.0.0.1:5002")   # префикс tcp:// можно опустить
cache_uds = TinyCache("unix:///var/lib/tiny-mp-cache/ipc/tiny-cache.sock")
```

- Ключи — строки (`str`), значения — байты (`bytes`). Сериализацию объектов (JSON, pickle и т.п.) контролирует приложение.
- Клиент подключается в конструкторе и держит одно постоянное соединение. Если сервер ещё не готов, конструктор бросает `RuntimeError`.
- Создавайте `TinyCache` в каждом процессе после `fork`, а не до него: иначе процессы будут делить один сокет.
- Размер команды (ключ + значение) ограничен 1 МиБ; больше — `ValueError` без отправки на сервер.

### TinyCache(addr, retries=None, retry_delay_ms=None)

`retries` — число попыток на операцию (по умолчанию 1, то есть без повторов); между попытками клиент переподключается и ждёт `retry_delay_ms` (по умолчанию 100 мс). Менять можно и после создания: `set_retries(n)`, `set_retry_delay_ms(ms)`.

### set(key: str, value: bytes, ttl: float | None = None) -> None

Сохраняет значение. `ttl` — время жизни в секундах; `None` — без ограничения.

```python
cache.set("user:1", b"payload")
cache.set("otp:42", b"123456", ttl=60)
```

### get(key: str) -> Optional[bytes]

Возвращает значение или `None`, если ключа нет или истёк его TTL.

### pop(key: str) -> Optional[bytes]

Атомарно забирает значение и удаляет ключ. Если несколько воркеров одновременно вызывают `pop` для одного ключа, значение получит ровно один из них.

```python
job_raw = cache.pop("job:123")
if job_raw is not None:
    job = job_raw.decode("utf-8")
```

### delete(key: str) -> int

Удаляет ключ: `1` — ключ был удалён, `0` — ключа не было.

### keys(pattern: str) -> list[str]

Ключи, подходящие под паттерн. Поддерживается только префиксный паттерн `"prefix*"`; для паттерна без `*` в конце возвращается пустой список.

```python
jobs = cache.keys("job:*")
```

### len() -> int

Количество живых ключей.

***

## Персистентность

### Как устроено хранение

```
data_dir/
  meta/state.json          # текущий чанк, список сохранённых чанков, настройки
  meta/keys.bin            # мета ключей: key_id, чанк, оставшийся TTL
  chunks/chunk_<id>.bin    # значения ключей, сохранённые snapshot'ом
  wal/log_<id>.bin         # журнал операций чанка, ещё не сохранённого snapshot'ом
  ipc/tiny-cache.sock      # сокет serve_unix
```

Время делится на чанки по `snapshot_interval_secs`, `id` чанка — время его начала (unix, мс). Каждая операция `set`/`pop`/`delete` сначала пишется в WAL текущего чанка, затем применяется в памяти. Snapshot выполняется на границах, кратных интервалу:

1. запись переключается на новый чанк и новый сегмент WAL;
2. значения закрытого чанка сохраняются в `chunks/chunk_<id>.bin`, затем `keys.bin` и `state.json`;
3. только после этого удаляется сегмент WAL закрытого чанка.

При старте сервер загружает мету и все chunk‑файлы, затем доигрывает оставшиеся сегменты WAL. Падение на любом шаге (в том числе `kill -9`) не теряет подтверждённые сервером записи; недописанная последняя запись WAL отбрасывается.

### Retention и TTL — разные вещи

- **TTL** задаётся при записи ключа (`set(..., ttl=...)`) и определяет, когда ключ пропадает.
- **Retention** — настройка сервера (`retention_secs`), которая ограничивает только количество chunk‑файлов на диске. Когда чанк становится старше `retention_secs`, его живые ключи переписываются в очередной snapshot, а старый файл удаляется. Ключи из-за retention **не удаляются**.

Ключ пропадает только по TTL, `delete` или `pop`.

### Ограничения

- TTL хранится как длительность, а не как момент истечения, поэтому время простоя сервера в TTL не засчитывается. После рестарта у ключей из snapshot'а отсчитывается остаток TTL на момент snapshot'а, а у ключей, доигранных из WAL, TTL начинается заново целиком.
- Запись в WAL не делает `fsync`: падение процесса безопасно, а при внезапном отключении питания могут потеряться последние операции.
- Все команды обрабатываются последовательно одним рабочим потоком сервера. Это даёт атомарность (`pop`), но ограничивает пропускную способность одним ядром.

***

## Пример: продюсер и воркеры

Кэш как простая очередь задач между процессами: продюсер кладёт задачи, воркеры разбирают их через `pop`. Полный вариант с проверкой «каждая задача ровно один раз» для TCP и UDS — `tests/full_test.py`.

```python
import json
import multiprocessing as mp
import tempfile
import time
from tiny_mp_cache import serve, TinyCache

PORT = 5002
ADDR = f"tcp://127.0.0.1:{PORT}"
N_JOBS = 1000
N_WORKERS = 8


def producer():
    cache = TinyCache(ADDR)
    for i in range(N_JOBS):
        cache.set(f"job:{i}", json.dumps({"id": i}).encode())


def worker(worker_id: int):
    cache = TinyCache(ADDR)  # своё соединение в каждом процессе
    processed = 0
    while keys := cache.keys("job:*"):
        for key in keys:
            raw = cache.pop(key)  # None, если задачу уже забрал другой воркер
            if raw is not None:
                processed += 1
    print(f"[WORKER {worker_id}] processed {processed} jobs")


def main():
    mp.set_start_method("fork", force=True)
    with tempfile.TemporaryDirectory() as data_dir:
        srv = mp.Process(target=serve, args=(data_dir, PORT))
        srv.start()
        time.sleep(0.5)

        producer()
        workers = [mp.Process(target=worker, args=(w,)) for w in range(N_WORKERS)]
        for p in workers:
            p.start()
        for p in workers:
            p.join()

        srv.terminate()
        srv.join()


if __name__ == "__main__":
    main()
```

***

## Миграция с 0.1

| 0.1 | 0.2 |
|---|---|
| `serve(port, wal_dir=None)` | `serve(data_dir, port=5002, snapshot_interval_secs=60, retention_secs=600)` |
| `serve_unix(path, wal_dir=None)` | `serve_unix(data_dir, ...)`, сокет — `<data_dir>/ipc/tiny-cache.sock` |
| `tiny-mp-cache.wal` в `wal_dir` или текущей директории | `data_dir/` со структурой выше |
| новое соединение на каждую операцию | постоянное соединение, `retries` / `retry_delay_ms` |
| `set(key, value)` | `set(key, value, ttl=None)` |

- Формат данных изменился: журнал 0.1 (`tiny-mp-cache.wal`) версией 0.2 не читается. Кэш после обновления стартует пустым.
- `serve`/`serve_unix` теперь первым аргументом принимают `data_dir`: вызов `serve(5002)` из 0.1 завершится ошибкой типа.
- Сервер и клиент должны быть одной версии: протокол 0.1 и 0.2 несовместим.

***

## Запуск тестов

Перед запуском активируйте venv, куда пакет установлен через `maturin develop` или из PyPI. Тесты запускаются из каталога `tests/`, чтобы исходники пакета в корне репозитория не перекрывали установленный модуль.

```bash
cd tests
python cache_api_test.py          # базовый API, TTL, лимит размера (TCP и UDS)
python persistence_test.py        # восстановление из snapshot'ов и WAL
python crash_consistency_test.py  # запись во время snapshot'ов + kill -9
python retention_test.py          # retention не удаляет живые ключи, TTL отдельно
python graceful_shutdown_test.py  # SIGTERM -> финальный snapshot, код выхода 0
python full_test.py               # очередь задач: каждая задача ровно один раз
```

Те же тесты запускаются в CI на Linux и macOS; на Windows CI проверяет сборку и импорт.
