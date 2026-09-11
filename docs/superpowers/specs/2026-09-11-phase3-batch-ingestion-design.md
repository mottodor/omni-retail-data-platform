# Phase 3 — Batch Ingestion: REST API + files/S3 (Design Spec)

- **Дата:** 2026-09-11
- **Статус:** Approved (design review пройден в диалоге)
- **Ветка:** `feature/phase3-batch-ingestion`
- **Связанные документы:** `ROADMAP.md` (Phase 3), `AGENTS.md` (§14–16, §47), `docs/adr/0001-project-architecture.md`, будущий `docs/adr/0002-mock-api-service.md`

---

## 1. Контекст и цель

Phase 0–2 завершены: репозиторий с CI, core lakehouse (PostgreSQL, MinIO, Polaris, Trino), OLTP-схема и детерминированный генератор. Phase 3 закрывает наиболее частые batch ingestion кейсы:

- **API-источники:** FX rates, marketing campaigns, delivery status (через mock-сервис с fault injection);
- **Файловые источники:** CSV supplier prices, JSON partner products, Parquet historical orders, XLSX (edge case).

Выход фазы — durable raw-представление в MinIO с манифестами, дедупликацией, карантином и backfill. Загрузка в Iceberg Bronze — вне scope (Phase 5).

## 2. Зафиксированные решения (design review)

| Решение | Выбор |
|---|---|
| Разбиение работы | 3 слайса / 3 PR (файловое ядро → mock API + клиенты → parquet/xlsx + backfill-полировка) |
| Структура модуля | A: разделяемое ядро `ingestion/common` + тонкие пакеты `files`/`api` |
| Mock API | FastAPI + uvicorn, отдельный compose-сервис, ADR 0002 |
| Реестр манифестов | JSON-объекты в MinIO (`archive/_manifests/`, `archive/_dedup/`) |
| S3-клиент | boto3 (pinned) |
| HTTP-клиент | httpx (pinned) |

## 3. Архитектура

```text
ФАЙЛЫ:
  supplier CSV / partner JSON / parquet / xlsx
    → s3://landing/<source>/incoming/<filename>          (drop zone: генератор или ручная загрузка)
    → copy → s3://landing/<source>/processing/<filename>  (транзит; маркер прерванного run)
    → валидация (schema + rows)
        ├─ ok        → s3://archive/<source>/<yyyy>/<mm>/<dd>/<filename>
        ├─ bad rows  → s3://rejected/<source>/<yyyy>/<mm>/<dd>/<filename>.badrows.<ext>
        └─ bad file  → s3://rejected/<source>/<yyyy>/<mm>/<dd>/<filename>
    → манифест:      s3://archive/_manifests/<source>/<batch_id>.json
    → дедуп-маркер:  s3://archive/_dedup/<source>/<sha256>.json

API:
  mock-api (compose, profile core)
    → клиенты fx / marketing / delivery (pagination, retry, rate limit)
    → raw page JSON: s3://archive/api/<source>/<yyyymmdd>/page_XXXX.json
      (страницы — уже финальные payload'ы: durable raw сразу, без транзитной processing-стадии)
    → манифест батча: s3://archive/_manifests/<source>/<batch_id>.json
```

## 4. Структура модулей

```text
src/omni_retail/
├── ingestion/
│   ├── common/
│   │   ├── storage.py      # тонкая обёртка boto3: get/put/copy/head/delete/list
│   │   ├── checksum.py     # chunked sha256
│   │   ├── manifest.py     # BatchManifest (frozen dataclass) + реестр в S3
│   │   ├── paths.py        # конструкторы детерминированных S3-ключей
│   │   └── logging.py      # структурированный контекст (batch_id, source, counts)
│   ├── files/
│   │   ├── flow.py         # состояния объекта: incoming→processing→archive|rejected
│   │   ├── schemas.py      # декларативные схемы источников (CSV/JSON/parquet/xlsx)
│   │   ├── validation.py   # файловая + построчная валидация
│   │   └── cli.py          # python -m omni_retail.ingestion.files process ...
│   └── api/
│       ├── client.py       # httpx: timeout, retry+backoff+jitter, rate limit
│       ├── fx.py
│       ├── marketing.py
│       ├── delivery.py     # типизированные модели, raw-сохранение страниц
│       └── cli.py          # run --source ... --date | backfill --from --to
├── generators/
│   └── vendor_files/       # детерминированные генераторы файлов (seed)
infrastructure/
└── mock_api/               # FastAPI приложение + Dockerfile
```

Конвенции — как в `generators.oltp`: frozen dataclass-конфиги с `validate()`, чистые функции для логики, тонкий IO-слой, mypy strict.

## 5. Идемпотентность, batch_id, дедупликация

- **Файлы:** `batch_id = <source>-<sha256[:16]>` — контентно-адресуемый. Повторная загрузка того же содержимого: HEAD `archive/_dedup/<source>/<sha256>.json` → существует → skip со статусом `duplicate` в логах/манифесте. Повторный запуск после сбоя на середине: объект в `processing` без финального статуса репроцессится; `incoming` удаляется только после успешного archive/reject.
- **API:** `batch_id = <source>-<yyyymmdd>` (logical date). Ключи страниц детерминированы (`page_0001.json`...), повторный run того же date перезаписывает те же объекты — дублей нет.
- **Backfill:** только через явные параметры `--date` / `--from`/`--to`; wall-clock `now()` не используется нигде в путях/идентификаторах.

## 6. Манифест батча

`BatchManifest` (frozen dataclass, сериализация в JSON):

- `batch_id`, `source`, `source_kind` (file|api), `status` (completed|rejected|duplicate);
- `object_key` (архивный/raw путь), `checksum` (sha256), `size_bytes`;
- `ingested_at` (UTC), `logical_date` (для API);
- `row_count`, `rejected_row_count`;
- `schema_version` источника;
- `rejection_reasons` (для rejected: файловые причины; для bad rows — агрегированные причины).

## 7. Политики ошибок (явные)

| Сценарий | Поведение |
|---|---|
| Дубликат файла (тот же sha256) | skip, статус `duplicate`, log + manifest |
| Битая структура файла (нечитаем, не та схема) | весь файл → `rejected` + `.rejection.json` с причиной |
| Битые строки при валидной структуре | строки → `<file>.badrows.<ext>` (+причины), файл архивируется, манифест фиксирует счётчики; строгий режим `--fail-on-rejected` валит run |
| API 429/5xx/timeout | retry с экспоненциальным backoff + jitter, уважение `Retry-After`; max attempts → fail батча (уже сохранённые raw-страницы остаются) |
| API 4xx non-retryable (404/422) | fail сразу с явной ошибкой |
| Прерванный run (объект застрял в `processing`) | репроцессинг при следующем запуске, дублей не создаёт |
| S3 объект исчез между шагами | явная ошибка, batch fail, nothing archived |

Retry-классификация: retryable = HTTP 429, 5xx, timeout, connection error; non-retryable = остальные 4xx. Бесконечных retry нет: max attempts (default 5) + общий timeout.

## 8. Mock API сервис

- **ADR 0002** перед реализацией (новый сервис — требование AGENTS.md §11/§42).
- FastAPI + uvicorn, pinned версии, Dockerfile на `python:3.12-slim` (pinned digest/tag), compose profile `core`, порт `127.0.0.1:9002`, healthcheck `/healthz`.
- Эндпоинты:
  - `GET /api/v1/fx-rates?base=EUR&date=YYYY-MM-DD&page=&page_size=` — offset-пагинация;
  - `GET /api/v1/marketing/campaigns?status=&page=&page_size=` — offset-пагинация;
  - `GET /api/v1/deliveries?updated_since=&cursor=&limit=` — cursor-пагинация.
- Данные детерминированы (seed + date) → backfill воспроизводим.
- Fault injection: query-параметры `?fault=429|500|timeout&fault_rate=0.5` — детерминированно по (seed, request).

## 9. Слайсы

| Слайс | Содержимое | PR |
|---|---|---|
| 1 | `ingestion/common` (storage, checksum, manifest, paths, logging) + `ingestion/files` (flow, schemas CSV/JSON, validation, CLI) + `generators/vendor_files` CSV/JSON + make-таргеты + fake-storage unit-тесты + `.env.example`/minio_init.sh (least-privilege пользователь `omni-ingestion`) | #3 |
| 2 | ADR 0002 + `infrastructure/mock_api` + compose-сервис + guard-тесты + `ingestion/api` (client + fx/marketing/delivery + CLI run/backfill) + unit-тесты на MockTransport | #4 |
| 3 | Parquet + XLSX (pyarrow, openpyxl) в генератор и валидацию + integration-тесты против живого core (`make integration`, гейт `OMNI_INTEGRATION=1`) + `docs/data-contracts.md` (черновик) + runbook «bad supplier file» | #5 |

## 10. Зависимости

Runtime (ranged pins, как `psycopg[binary]>=3.2,<4`):

- слайс 1: `boto3`;
- слайс 2: `httpx`;
- слайс 3: `pyarrow`, `openpyxl`;
- fastapi/uvicorn — только внутри образа mock-api, не в основном пакете.

Dev-зависимости не добавляются: fake in-memory storage вместо moto; httpx `MockTransport` для retry-тестов.

## 11. Конфигурация

Новые переменные (→ `.env.example`, проверяется guard-тестом):

- `S3_ENDPOINT_URL` (default `http://127.0.0.1:9000`);
- `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` — отдельный least-privilege пользователь MinIO `omni-ingestion` (создаётся в `minio_init.sh`, политика: rw на `landing/archive/rejected`, r на `lakehouse`);
- `MOCK_API_BASE_URL` (default `http://127.0.0.1:9002`), `MOCK_API_PORT` (compose).

Makefile: `seed-supplier-files`, `ingest-files`, `ingest-api`, `integration`.

## 12. Тестирование

- **Unit (pytest, детерминированные фикстуры):** fake in-memory S3-клиент (типизированный, без сети); детерминизм batch_id/ключей/checksum; валидаторы на good/bad фикстурах; retry-классификация и пагинация через httpx MockTransport; идемпотентность повторного run.
- **Guard-тесты compose:** pinned base image, healthcheck, `.env.example` покрытие, loopback-only порты — для `mock-api`.
- **Integration (слайс 3):** живые MinIO + mock-api; сценарии: повторный batch без дублей, битый файл → rejected, 429 → retry → успех, backfill диапазона.

## 13. Out of scope

- Загрузка в Iceberg Bronze / dbt (Phase 5);
- Airflow-оркестрация (Phase 4);
- реестр манифестов в PostgreSQL (пересмотр при приходе Airflow/SLA);
- REST API аутентификация в клиентах (mock не требует; добавим при реальном источнике).

## 14. Соответствие acceptance criteria ROADMAP (Phase 3)

| Критерий | Чем закрывается |
|---|---|
| Повторный запуск одного batch не создаёт дублей | контентно-адресуемые batch_id + дедуп-маркеры + детерминированные ключи страниц (§5, §12) |
| Сломанный файл попадает в rejected | flow + validation + `.rejection.json` (§3, §7) |
| API 429/500 корректно retry-ится | retry-классификация + backoff + fault injection (§7, §8) |
| Backfill за диапазон дат | CLI `backfill --from --to` на logical dates (§5, слайс 2–3) |
| raw payload сохраняется неизменным | archive raw / raw page JSON без трансформаций (§3) |
| metadata (source, ts, batch_id, file, checksum) | BatchManifest (§6) |
