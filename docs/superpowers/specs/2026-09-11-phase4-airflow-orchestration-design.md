# Phase 4 — Airflow 2.11.2 Orchestration (Design Spec)

- **Дата:** 2026-09-11
- **Статус:** Approved (design review пройден в диалоге)
- **Ветка:** `feature/phase4-airflow-orchestration`
- **Связанные документы:** `ROADMAP.md` (Phase 4), `AGENTS.md` (§19, §30, §12, §51), `docs/adr/0001-project-architecture.md`, будущий `docs/adr/0003-airflow-deployment.md`, `docs/superpowers/specs/2026-09-11-phase3-batch-ingestion-design.md`

---

## 1. Контекст и цель

Phase 0–3 завершены: core lakehouse (PostgreSQL, MinIO, Polaris, Trino, mock-api), OLTP-генератор, batch ingestion CLI (3 API-источника + 4 файловых источника) с манифестами, дедупликацией, карантином и backfill.

Phase 4 выносит orchestration в Airflow **без смешивания с transformation logic** (AGENTS §19.1): DAG-и — тонкие обёртки над существующими ingestion-функциями; их SQL/логика не переезжают в DAG.

Scope (утверждён в design review):

- 5 DAG: `ingest_fx_api`, `ingest_marketing_api`, `ingest_delivery_api`, `ingest_supplier_files`, `ingest_postgres_snapshot`;
- новый модуль экстракции PostgreSQL → landing (Parquet, watermark) — реализация под `ingest_postgres_snapshot`;
- `lakehouse_bronze_ready` (dataset-triggering) — **отложен до Phase 5**, когда появится загрузка в Bronze.

## 2. Зафиксированные решения (design review)

| Решение | Выбор |
|---|---|
| Executor | LocalExecutor; без Celery/Redis/triggerer (overengineering для одной ноды WSL2, AGENTS §53) |
| Metadata БД | отдельный `airflow-postgres` + volume `airflow-metadata-data` (не core-PostgreSQL: `make reset` уничтожает `postgres-data` вместе с метаданными) |
| Исполнение ingestion-кода | custom-образ `omni-retail/airflow:0.1.0` на `apache/airflow:2.11.2-python3.12`, пакет `omni_retail` запечён; TaskFlow-задачи вызывают ingestion-функции в worker-процессе |
| Воспроизводимость образа | версии зависимостей экспортируются из закоммиченного `uv.lock` (`uv export --frozen --no-dev`), `uv pip install --system`; резолв заново не выполняется |
| Конфигурация задач | env compose (единый источник с локальными запусками), без дублирования в Airflow Connections; Airflow Variables не используются |
| Secrets | только env / `.env` (`FERNET_KEY`, admin, пароли БД) |
| Watermark PG-снапшота | JSON-объект в MinIO `archive/_watermarks/postgres/<table>.json` — по образцу реестров Phase 3; не Airflow metadata |
| Новые зависимости | нет: `psycopg` и `pyarrow` уже в `uv.lock` (Phase 3) |
| ADR | `docs/adr/0003-airflow-deployment.md` до реализации (модель развёртывания: LocalExecutor, custom image, отдельный metadata-PG, env-based secrets) |

## 3. Архитектура

```text
profile orchestration:
  airflow-postgres (postgres:16.15-alpine, volume airflow-metadata-data, без host-порта)
  airflow-init     (one-shot: db migrate + admin user + pool "mock_api"; condition: postgres healthy)
  airflow-webserver (127.0.0.1:8081:8080, healthcheck GET /health)
  airflow-scheduler (LocalExecutor)

  требует поднятого profile core (postgres, minio, mock-api) — документировано;
  жёстких cross-profile depends_on в compose нет

DAG-и (airflow/dags/*.py, тонкие):
  вызывают helpers из airflow/include/runners.py
    → существующие функции src/omni_retail/ingestion/{api,files,postgres_snapshot}
    → MinIO (landing/archive/rejected) + mock-api + PostgreSQL OLTP
```

Монтирование: `./airflow/dags:/opt/airflow/dags:ro`, `./airflow/include:/opt/airflow/include:ro`, `./airflow/tests:/opt/airflow/tests:ro`; `PYTHONPATH=/opt/airflow` (пакет `include` импортируется как `include.*`).

## 4. DAG-и

| DAG | Schedule | Задачи (TaskFlow) | Особенности |
|---|---|---|---|
| `ingest_fx_api` / `ingest_marketing_api` / `ingest_delivery_api` | `@daily` | `ingest(logical_date)` → dict summary | factory `build_api_ingestion_dag`; pool `mock_api`; summary (batch_id, status, row_count, object_key) → XCom |
| `ingest_supplier_files` | `@daily` | 4 параллельные задачи — по одному файловому источнику | `fail_on_rejected` — DAG param (default `False`); падение одного источника не останавливает остальные |
| `ingest_postgres_snapshot` | `@daily` | 7 параллельных задач — по таблице | param `full_refresh` (default `False`) |

Общие `default_args` / DAG-уровень:

- `retries=3`, `retry_delay=30s`, `retry_exponential_backoff=True`, `retry_max_delay=5min`;
- `execution_timeout=5min` на задачу;
- `max_active_runs=1`, `catchup=False`, `start_date=2026-09-01`;
- `AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=True` — новые DAG стартуют на паузе;
- логическая дата (`ds`) передаётся в ingestion как `--date`; wall-clock `now()` не используется нигде в путях/идентификаторах (AGENTS §30);
- retry на уровне задач — дополнение к retry внутри API-клиента (http-level) и S3-операций, а не замена.

Backfill: `make airflow-backfill ARGS="ingest_fx_api -s 2026-09-01 -e 2026-09-10"` → `airflow dags backfill` внутри контейнера scheduler.

## 5. Экстракция PostgreSQL → landing

Новый модуль `src/omni_retail/ingestion/postgres_snapshot/`:

```text
config.py     PostgresSourceConfig.from_env() — существующие POSTGRES_* переменные
tables.py     декларативные TableSpec: имя, pk, колонки + типы pyarrow, schema_version
                (7 таблиц: categories, customers, products, orders, order_items,
                 payments, shipments) — по образцу files/schemas.py
extract.py     keyset-пагинация (updated_at, pk), server-side cursor, fetchmany
                → pyarrow Table → parquet bytes
watermark.py   чтение/запись s3://archive/_watermarks/postgres/<table>.json
cli.py         run --table <t> --date <d> [--full-refresh]  (+ __main__.py)
```

Семантика:

- нет watermark или `--full-refresh` → полный снимок (bootstrap ≈115k строк суммарно — приемлемо для одной ноды); далее `WHERE updated_at > watermark ORDER BY updated_at, pk`;
- выгрузка: `s3://archive/postgres/<table>/<yyyy>/<mm>/<dd>/data.parquet` (перезапись того же логического дня) + манифест в существующем реестре `archive/_manifests/`; `batch_id = postgres-<table>-<yyyymmdd>` (детерминирован); `source_kind` манифеста расширяется значением `postgres` (backward-compatible);
- watermark обновляется **только после** успешной загрузки → обрыв между upload и watermark даёт повторное извлечение того же окна с перезаписью того же ключа (дублей нет);
- пустое окно: манифест со статусом `completed`, `row_count=0`, parquet не пишется;
- порождение клиентом — `psycopg` (connection из `PostgresSourceConfig`), сериализация — явные pyarrow-схемы из `tables.py` (детерминизм, тестируемость).

## 6. Идемпотентность и backfill

- **API/файлы:** повторный запуск того же logical date перезаписывает детерминированные ключи объектов и тот же `batch_id` (доказано integration-тестами Phase 3); `airflow dags backfill` не создаёт дублей по построению.
- **PG-снапшот:** повторный run той же даты после успеха → пустое окно (`row_count=0`); повторный run после сбоя → перезапись того же parquet-ключа.
- **Ограничение (документируется):** PG-снапшот — это снимки текущего состояния source; исторический backfill по датам невозможен (источник не хранит историю) — закрывается CDC в Phase 8.

## 7. Политики ошибок (явные)

| Сценарий | Повтрение | Идемпотентность / поведение |
|---|---|---|
| API 429/5xx/timeout | http-level retry внутри клиента + task-level retries (Airflow) с exp backoff | уже сохранённые raw-страницы остаются; повторный run перезапишет те же ключи |
| unavailable mock-api / minio / postgres | task retries (3×, exp backoff) | пайплайны независимы: падение одного DAG не влияет на другие; pool `mock_api` ограничивает конкурентность |
| битый файл / bad rows | не retry-ится | карантин `rejected` + причины; task-успех (log summary), `fail_on_rejected=True` валит задачу |
| сбой записи watermark после upload | task retry | повторное извлечение того же окна, перезапись parquet-ключа, дублей нет |
| interrupt задачи посреди экстракции | task retry | watermark не двигался → то же окно заново |
| пустое окно инкремента | — | `completed`, `row_count=0`, объект не пишется |

Hard-deletes невидимы для снапшота (документированное ограничение → CDC Phase 8). События в ту же секунду, что watermark, могут быть пропущены (строго `>`); опциональный overlap-параметр (default 0) — защита при необходимости.

## 8. Инфраструктура Airflow (compose)

- `infrastructure/airflow/Dockerfile` (новый, собирается в `omni-retail/airflow:0.1.0`):
  - `FROM apache/airflow:2.11.2-python3.12`;
  - pinned `uv` (copy from `ghcr.io/astral-sh/uv` с pinned tag);
  - `COPY pyproject.toml uv.lock src/` → `uv export --frozen --no-dev` → `uv pip install --system -r` (pinned-зависимости) → `uv pip install --system --no-deps .` (сам пакет `omni_retail` без резолва) + pinned `pytest` для DAG-тестов;
  - `USER airflow`.
- `airflow-init`: `airflow db migrate`, создание admin (env `_AIRFLOW_WWW_USER_*`), `airflow pools set mock_api 2 "mock external API concurrency limit"`.
- Env сервисов: `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow-postgres:5432/...`, `AIRFLOW__CORE__FERNET_KEY=${AIRFLOW_FERNET_KEY}`, плюс ingestion-env с docker-network адресами: `S3_ENDPOINT_URL=http://minio:9000`, `MOCK_API_BASE_URL=http://mock-api:9002`, `POSTGRES_HOST=postgres`.
- Healthchecks: webserver `GET /health`; scheduler `airflow jobs check --job-type SchedulerJob --hostname $(hostname)`; init — `service_completed_successfully`.
- Скрипт `infrastructure/scripts/airflow_tests.sh` — общий для `make airflow-test` и CI.

## 9. Слайсы

| Слайс | Содержимое | PR |
|---|---|---|
| 1 | ADR 0003 + Dockerfile + compose-сервисы profile `orchestration` + Make-таргеты (`airflow-up/down/test/backfill/dag-test`) + guard-тесты compose + DAG import test harness (DagBag: без import errors, dag_ids, задачи, deps, retries/pool/timeout) + CI job (build образа → тесты внутри) + пилотный DAG `ingest_fx_api` + runners/factory | #6 |
| 2 | DAG-и `ingest_marketing_api`, `ingest_delivery_api` (через factory) + `ingest_supplier_files` (4 параллельных задачи, param `fail_on_rejected`) + расширенные DAG-тесты | #7 |
| 3 | Модуль `ingestion/postgres_snapshot` (config/tables/extract/watermark/cli) + unit-тесты (fake cursor + fake storage) + DAG `ingest_postgres_snapshot` (param `full_refresh`) + integration-тест против живого core (parquet + manifest + watermark; повторный run без дублей; инкремент после мутации) + README/`.env.example` финализация | #8 |

## 10. Зависимости

Новых нет. Runtime уже в `uv.lock`: `psycopg[binary]`, `pyarrow`, `httpx`, `boto3`, `openpyxl`. В образ Airflow они попадают из lock-файла; пересечений с constraints Airflow нет (не входят в его pinned-набор).

## 11. Конфигурация

Новые переменные (→ `.env.example`, проверяется guard-тестом):

- `AIRFLOW_UID` (default 50000);
- `AIRFLOW_FERNET_KEY` (placeholder + инструкция генерации);
- `AIRFLOW_WWW_USER` / `AIRFLOW_WWW_PASSWORD` (локальный admin UI);
- `AIRFLOW_WEBSERVER_PORT` (default 8081, loopback-only);
- `AIRFLOW_POSTGRES_USER` / `AIRFLOW_POSTGRES_PASSWORD` / `AIRFLOW_POSTGRES_DB` (metadata, без host-порта).

Ingestion-env сервисов Airflow переопределяется на docker-network адреса (compose environment), значения секретов — из тех же `${S3_*}`, `${POSTGRES_*}`, `${MOCK_API_*}`.

## 12. Тестирование

- **Unit (dev venv, без Airflow):** watermark-логика (fake storage), SQL keyset-пагинация (fake cursor), парquet-сериализация (типы из `TableSpec`), config parsing, CLI; по образцу Phase 3.
- **DAG-тесты (внутри образа):** DagBag без import errors; ожидаемые `dag_ids`; структура задач и зависимости; `retries`, `execution_timeout`, `pool`, `catchup=False`, `max_active_runs=1`, schedule; params по умолчанию. Запуск — `make airflow-test`.
- **Guard-тесты compose (dev venv):** profile `orchestration`, pinned images, healthchecks, loopback-only порты, `.env.example` покрытие новых переменных, отсутствие host-порта у `airflow-postgres`.
- **CI:** новый job в `ci.yml` — `docker build` образа → запуск `airflow_tests.sh` в контейнере (pytest + `airflow dags list-import-errors`).
- **Integration (слайс 3, live core):** экстракция PG (контролируемые вставки/мутации) → parquet + manifest + watermark; повторный run → `row_count=0`, без дублей; DAG-level: `make airflow-dag-test ARGS="ingest_fx_api 2026-09-10"` (`airflow dags test`) — локальная ручная проверка.

## 13. Out of scope

- `lakehouse_bronze_ready` / Airflow Datasets (Phase 5, после появления Bronze);
- CeleryExecutor/Redis, deferrable operators, triggerer;
- custom auth backend, RBAC-политики поверх стандартного admin;
- CDC, historical backfill PG-таблиц (Phase 8);
- observability DAG-метрик в Prometheus (Phase 12);
- Airflow 3 migration (Phase 16).

## 14. Соответствие acceptance criteria ROADMAP (Phase 4)

| Критерий | Чем закрывается |
|---|---|
| DAG-и успешно парсятся в CI | DAG import tests job (build образа + DagBag) (§12) |
| Ручной backfill не создаёт дублей | детерминированные ключи/batch_id Phase 3 + watermark-семантика PG (§6) |
| Отдельное падение API не ломает независимые ingest pipeline | независимые DAG-и, task retries, pool `mock_api`, изоляция задач файлов (§4, §7) |
| Failure context виден в логах | structured logging ingestion (batch_id/source/counts) + XCom-summaries задач (§4) |
| TaskFlow API, retries/backoff, pools, execution timeout | §4 |
| Catchup/backfill стратегия, logical date | `catchup=False` + `airflow dags backfill` + `ds`→`--date` (§4, §6) |
| Variables только для non-secret, secrets через env | конфигурация полностью через env, Variables не используются (§2, §11) |
| DAG import tests | §12 |
