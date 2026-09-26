# OmniRetail Data Platform — подробный roadmap

**Назначение:** pet-project уровня production-like для отработки проектирования DWH/Lakehouse и ETL/ELT-процессов на Windows 11 + WSL2 с 32 GB RAM.

**Основной стек:** Apache Airflow 2.11.2, dbt Core 1.10.x (адаптер dbt-trino 1.9.x), Trino 476+ (целевой pinned release — 483), Apache Iceberg, PostgreSQL 16+, ClickHouse, Apache Superset, MinIO (S3 API), Apache Kafka, Debezium, Apache Spark, Apache Polaris, OpenLineage + Marquez, Prometheus + Grafana, Docker Compose, GitHub + GitHub Actions. Позже — GitLab CI и Kubernetes.

> Принцип проекта: сначала рабочая вертикаль end-to-end, затем усложнение. Каждая новая технология должна решать конкретную инженерную задачу, а не добавляться «для галочки».

---

## 1. Цели проекта

Проект должен дать опыт, максимально близкий к задачам Data Engineer в коммерческой команде:

- проектирование lakehouse/DWH-архитектуры;
- batch ingestion из S3-compatible storage, REST API и PostgreSQL;
- CDC из PostgreSQL через Debezium + Kafka;
- event streaming и обработка clickstream;
- Bronze/Silver/Gold слои в Apache Iceberg;
- ELT-моделирование через dbt Core + Trino;
- Kimball: facts, dimensions, SCD Type 2;
- небольшой Data Vault 2.0 кейс как дополнительный домен;
- ClickHouse как serving/OLAP-слой;
- Apache Superset как BI;
- data quality, idempotency, backfill, schema evolution, late-arriving data;
- Iceberg maintenance и small-files problem;
- observability, lineage и SLA/SLO;
- GitHub Actions CI, контейнеризация и воспроизводимое локальное окружение;
- документирование архитектурных решений (ADR) и runbooks;
- финальная миграционная задача Airflow 2 -> Airflow 3 и позже GitHub Actions -> GitLab CI.

## 2. Бизнес-сценарий

**OmniRetail** — условная e-commerce компания. Есть интернет-магазин, мобильное приложение, рекламные каналы, платежи, доставка и внешние поставщики.

Основные бизнес-вопросы:

1. Как меняются GMV, Revenue, Margin и AOV?
2. Какова конверсия funnel: visit -> product_view -> cart -> checkout -> purchase?
3. Какие продукты и категории дают максимальную маржу?
4. Как работают рекламные кампании: CTR, CAC, ROAS?
5. Какова retention/LTV клиентов?
6. Какие доставки опаздывают?
7. Есть ли расхождения между заказами и платежами?

## 3. Финальная архитектура

```text
SOURCES
  PostgreSQL OLTP ---- Debezium ---- Kafka -------------------+
  REST APIs ---------------- Airflow/Python ------------------+
  S3/CSV/JSON/Parquet ------- Airflow ------------------------+--> MinIO/S3
  Web/App events ------------ Kafka --------------------------+      |
                                                                  Iceberg
                                                         Bronze -> Silver -> Gold
                                                                    |
                                                          Trino + dbt Core
                                                                    |
                                               +--------------------+------------------+
                                               |                                       |
                                          Iceberg Gold                           ClickHouse
                                           source of truth                       serving layer
                                                                                      |
                                                                                  Superset

ORCHESTRATION: Airflow 2.11.2
CATALOG: Apache Polaris (Iceberg REST catalog)
BIG DATA: Spark для тяжелых файлов/clickstream/sessionization
QUALITY: dbt tests + custom SQL/Python reconciliation
LINEAGE: OpenLineage + Marquez
MONITORING: Prometheus + Grafana
DEVOPS: GitHub + GitHub Actions + Docker Compose
LATER: GitLab CI, Kubernetes, Airflow 3 migration
```

### 3.1 Разделение ответственности

| Компонент | Роль |
|---|---|
| PostgreSQL 16+ | OLTP source и отдельные служебные БД |
| Debezium | CDC из WAL PostgreSQL |
| Kafka | event transport / CDC / clickstream |
| MinIO | локальный S3-compatible object storage |
| Iceberg | table format и source of truth для lakehouse |
| Polaris | Iceberg REST catalog |
| Trino | SQL compute/federation над Iceberg и другими источниками |
| dbt Core | SQL transformations, tests, documentation |
| Spark | тяжелая distributed processing, sessionization, backfills |
| ClickHouse | low-latency serving layer для BI |
| Superset | BI и аналитические dashboards |
| Airflow | orchestration и dependency management |
| Prometheus/Grafana | platform observability |
| OpenLineage/Marquez | data lineage |
| GitHub Actions | CI и сборка артефактов |

## 4. Ограничения среды

Целевая машина: Windows 11 + WSL2, 32 GB RAM.

Рекомендуемый лимит WSL2:

```ini
[wsl2]
memory=24GB
processors=6
swap=8GB
```

Не запускать весь стек постоянно. Docker Compose разбить на profiles:

- `core`: PostgreSQL, MinIO, Polaris, Trino;
- `orchestration`: Airflow + metadata PostgreSQL + Redis при необходимости;
- `streaming`: Kafka + Kafka Connect/Debezium;
- `spark`: Spark master/worker;
- `observability`: Prometheus, Grafana, Marquez;
- `bi`: ClickHouse + Superset.

## 5. Структура репозитория

Фактическая структура репозитория (src-layout: Python-код живет в пакете `src/omni_retail`):

```text
omni-retail-data-platform/
├── .github/
│   └── workflows/
│       └── ci.yml
├── airflow/
│   ├── dags/
│   ├── include/
│   └── tests/
├── dbt/
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── core/
│   │   └── marts/
│   └── tests/
├── infrastructure/
│   ├── airflow/            # кастомный образ Airflow (Dockerfile, ADR 0003)
│   ├── mock_api/           # mock API сервис (ADR 0002)
│   └── scripts/            # minio/polaris init, smoke-тесты
├── postgres/
│   └── init/               # идемпотентный OLTP DDL
├── src/
│   └── omni_retail/        # Python-пакет (src-layout)
│       ├── generators/     # oltp/, vendor_files/
│       ├── ingestion/      # api/, files/, postgres_snapshot/, common/
│       └── lakehouse/      # bronze/
├── tests/
│   ├── fakes/
│   ├── integration/
│   └── unit/
├── trino/
│   └── etc/                # конфигурация Trino
├── docs/
│   ├── adr/
│   ├── runbooks/
│   └── ...                 # data-model.md, data-contracts.md и т.д.
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── uv.lock
├── .env.example
├── AGENTS.md
├── ROADMAP.md
└── README.md
```

Каталоги будущих фаз создаются только при наступлении соответствующей фазы, не заранее:

- Phase 6: `clickhouse/` (migrations, tests);
- Phase 7: `superset/` (dashboards, config);
- Phase 8: `kafka/` (producers, schemas, connect);
- Phase 9: `spark/` (jobs, tests) + `src/omni_retail/generators/clickstream/`;
- Phase 12: `observability/` (prometheus, grafana, marquez);
- `postgres/migrations/` — при появлении версионных миграций OLTP-схемы.

## 6. Правила работы с AI-агентом

AI-агент не должен реализовывать весь roadmap одним большим изменением. Каждая задача — отдельная небольшая итерация с тестами.

### 6.1 Обязательный цикл агента

1. Прочитать `AGENTS.md`, `ROADMAP.md`, релевантный ADR и текущий код.
2. Сформулировать изменяемый scope.
3. Не менять соседние компоненты без необходимости.
4. Реализовать минимально достаточное решение.
5. Добавить/обновить unit/integration tests.
6. Запустить локальные проверки.
7. Обновить документацию и `.env.example`, если появились новые настройки.
8. Сообщить: что изменено, как проверить, известные ограничения.

### 6.2 Definition of Done для любой задачи

- код воспроизводимо запускается из чистого clone;
- секреты не находятся в Git;
- конфигурация вынесена в env/config;
- есть healthcheck, если добавлен сервис;
- есть тесты для критической логики;
- pipeline идемпотентен там, где это требуется;
- логи содержат `run_id` / `batch_id` / dataset context;
- документация обновлена;
- `make lint`, `make test` и релевантные integration checks проходят.

### 6.3 Запреты для агента

- не писать SQL-transformations внутрь Airflow DAG, если они относятся к dbt layer;
- не использовать Spark для маленьких объемов без причины;
- не хранить raw API response только в памяти — сохранять raw payload в Bronze/landing;
- не делать `SELECT *` для production-like incremental extraction без объяснения;
- не использовать `latest` Docker tags в закрепленном окружении;
- не добавлять новый сервис без ADR с причиной и trade-offs;
- не объединять ingest, transform и serving в один DAG/task;
- не отключать тесты ради прохождения CI.

---

# 7. Roadmap реализации

## Phase 0 — Bootstrap и инженерные стандарты

**Цель:** создать репозиторий, в котором дальнейшие изменения контролируются автоматически.

### Задачи

- создать публичный GitHub repository;
- настроить Python project через `pyproject.toml`;
- определить поддерживаемую Python-версию (рекомендуется 3.12 для совместимости с Airflow 2.11.2);
- добавить `ruff`, `pytest`, `mypy` (постепенно), `pre-commit`;
- создать `.env.example` и правила secrets management;
- Makefile: `setup`, `lint`, `test`, `up`, `down`, `logs`, `reset`;
- создать `AGENTS.md`, ADR template, PR template;
- настроить GitHub Actions `ci.yml`.

### CI v1

```text
pull_request / push
   |
   +-- ruff check
   +-- pytest unit
   +-- mypy (initially non-blocking if needed)
   +-- docker compose config
```

### Acceptance criteria

- clone -> documented setup -> `make test` проходит;
- CI автоматически запускается на PR;
- repository не содержит secrets;
- README содержит dev prerequisites.

### Артефакты

`README.md`, `AGENTS.md`, `pyproject.toml`, `Makefile`, `.github/workflows/ci.yml`, `docs/adr/0001-project-architecture.md`.

---

## Phase 1 — Core infrastructure: PostgreSQL + MinIO + Polaris + Trino + Iceberg

**Цель:** получить минимальный работающий lakehouse.

### Задачи

- Docker Compose network и volumes;
- PostgreSQL 16+ как OLTP source;
- MinIO buckets: `landing`, `lakehouse`, `archive`, `rejected`;
- Apache Polaris как Iceberg REST catalog;
- Trino 476+ с pinned release;
- Iceberg catalog в Trino;
- healthchecks и dependency conditions;
- init scripts для bucket/database/schema creation.

### Проверочный сценарий

```sql
CREATE SCHEMA iceberg.demo;
CREATE TABLE iceberg.demo.healthcheck (...);
INSERT INTO iceberg.demo.healthcheck VALUES (...);
SELECT * FROM iceberg.demo.healthcheck;
```

### Acceptance criteria

- все core services поднимаются одной командой;
- Trino создает и читает Iceberg table в MinIO;
- после restart данные сохраняются;
- service credentials не захардкожены;
- `make smoke-core` проверяет end-to-end connectivity.

---

## Phase 2 — OLTP-модель и генератор данных

**Цель:** создать реалистичный источник транзакционных данных.

### Таблицы

- customers;
- products;
- categories;
- orders;
- order_items;
- payments;
- shipments.

### Требования

- PK/FK/constraints;
- `created_at`, `updated_at`;
- реалистичные статусы;
- generator с deterministic seed;
- режим initial load и continuous mutations;
- updates и deletes обязательны для будущего CDC;
- объемы на старте: 10k customers, 5k products, 100k orders.

### Acceptance criteria

- генератор воспроизводим;
- данные проходят constraints;
- можно генерировать update/delete workload;
- базовые source metrics документированы.

---

## Phase 3 — Batch ingestion: REST API + files/S3

**Цель:** закрыть наиболее частые batch ingestion кейсы.

### API sources

1. FX rates.
2. Marketing campaigns.
3. Delivery status API.

Для стабильности проекта внешние API можно обернуть mock-сервисом с реалистичным контрактом и fault injection.

### File sources

- CSV supplier prices;
- JSON partner products;
- Parquet historical orders;
- optional XLSX как отдельный edge case.

### Поток файлов

`landing -> processing -> archive` или `rejected`.

### Требования

- raw payload сохраняется неизмененным;
- metadata: source, ingestion timestamp, batch_id, file name, checksum;
- schema validation;
- duplicate-file detection;
- pagination, timeout, rate-limit, retry для API;
- quarantine для некорректных записей.

### Acceptance criteria

- повторный запуск одного batch не создает дублей;
- сломанный файл попадает в rejected;
- API 429/500 корректно retry-ится;
- можно сделать backfill за диапазон дат.

---

## Phase 4 — Airflow 2.11.2 orchestration

**Цель:** вынести orchestration в Airflow без смешивания с transformation logic.

### DAGs

- `ingest_fx_api`;
- `ingest_marketing_api`;
- `ingest_delivery_api`;
- `ingest_supplier_files`;
- `ingest_postgres_snapshot`;
- `lakehouse_bronze_ready` или dataset-triggering pattern.

### Требования

- TaskFlow API там, где уместно;
- retries/exponential backoff;
- pools для внешних API;
- execution timeout;
- catchup/backfill стратегия;
- параметры даты через logical date/data interval;
- Airflow Variables только для non-secret config; secrets через env/secret backend pattern;
- DAG import tests.

### Acceptance criteria

- DAG-и успешно парсятся в CI;
- ручной backfill не создает дублей;
- отдельное падение API не ломает независимые ingest pipeline;
- failure context виден в логах.

---

## Phase 5 — dbt + Trino: Bronze -> Silver -> Gold

**Цель:** реализовать основное аналитическое моделирование.

### dbt layers

**staging:** типизация, naming, минимальная очистка.

**intermediate:** deduplication, joins, normalization, business preparation.

**core:** dimensions/facts.

**marts:** бизнес-витрины.

### Kimball-модель

Dimensions:
- `dim_customer` (SCD2);
- `dim_product`;
- `dim_date`;
- `dim_campaign`.

Facts:
- `fact_orders`;
- `fact_order_items`;
- `fact_payments`;
- `fact_shipments`.

Marts:
- `mart_daily_sales`;
- `mart_customer_ltv`;
- `mart_marketing_roi`;
- `mart_delivery_performance`.

### dbt quality

- unique;
- not_null;
- relationships;
- accepted_values;
- source freshness;
- custom business tests.

### Acceptance criteria

- `dbt build` проходит;
- SCD2 корректно хранит историю;
- marts имеют documentation/description;
- lineage виден хотя бы на уровне dbt docs;
- бизнес reconciliation тестирует orders vs payments.

---

## Phase 6 — ClickHouse serving layer

**Цель:** отделить исторический lakehouse от low-latency BI-serving.

### Задачи

- создать `analytics` database;
- выбрать MergeTree engines;
- определить `ORDER BY`, partitioning и TTL только там, где оправдано;
- сделать incremental publishing из Gold Iceberg в ClickHouse;
- реализовать marts: sales, funnel, LTV, marketing, delivery;
- read-only user `superset_reader`.

### Что измерить

Сравнить один и тот же BI query:

- Trino -> Iceberg;
- ClickHouse serving table.

Зафиксировать latency и explain plan.

### Acceptance criteria

- публикация идемпотентна;
- ClickHouse можно полностью пересобрать из Iceberg Gold;
- Iceberg остается source of truth;
- BI user не имеет write permissions.

---

## Phase 7 — Apache Superset

**Цель:** создать полноценный BI-слой без внешнего сетевого доступа.

### Connections

1. Superset -> ClickHouse: основной dashboard source.
2. Superset -> Trino: ad-hoc exploration.

Для ClickHouse использовать рекомендуемый `clickhouse-connect` driver.

### Dashboards

1. Executive: GMV, revenue, margin, orders, AOV, conversion.
2. Sales: revenue by date/category/region, top products.
3. Customer: new/returning, retention, LTV, cohorts/RFM.
4. Marketing: impressions, clicks, CTR, CAC, ROAS.
5. Funnel: visit -> view -> cart -> checkout -> purchase.

### Acceptance criteria

- Superset подключается к ClickHouse по Docker network hostname;
- dashboards не требуют публикации ClickHouse наружу;
- metadata export хранится в репозитории;
- screenshots dashboards добавлены в README.

---

## Phase 8 — CDC: PostgreSQL -> Debezium -> Kafka -> Iceberg

**Цель:** заменить polling для ключевых OLTP-таблиц настоящим CDC.

### Задачи

- включить PostgreSQL logical replication;
- настроить Debezium PostgreSQL connector;
- topics для customers/orders/payments;
- хранение offsets/config;
- consumer/stream ingestion в Bronze;
- обработка create/update/delete;
- deduplication по event identity/LSN strategy;
- recovery после restart.

### Сценарии

- update order status;
- delete/soft-delete customer;
- duplicate event;
- consumer restart;
- schema change;
- out-of-order event.

### Acceptance criteria

- изменения source появляются в Bronze без full scan;
- повторная доставка не портит итоговое состояние;
- delete semantics документирована;
- restart не теряет committed events.

---

## Phase 9 — Clickstream + Spark

**Цель:** добавить workload, где Spark действительно оправдан.

### Events

- visit;
- product_view;
- search;
- add_to_cart;
- checkout;
- purchase.

### Pipeline

`Kafka/landing files -> Spark -> sessionization/dedup -> Iceberg Silver -> dbt Gold`.

### Обязательные темы

- event time vs processing time;
- late-arriving events;
- deduplication;
- session window;
- partition sizing;
- small files;
- Spark explain plan;
- broadcast vs shuffle join на отдельном benchmark.

### Acceptance criteria

- funnel строится из event data;
- late event корректно меняет нужную сессию в заданном окне;
- Spark job тестируется на deterministic fixture;
- memory/resource settings адаптированы к 32 GB ноутбуку.

---

## Phase 10 — Data Quality, contracts и failure engineering

**Цель:** научиться не только загружать данные, но и безопасно эксплуатировать pipelines.

### Data quality levels

**Source:** schema, nullability, volume, freshness.

**Pipeline:** duplicates, row counts, referential integrity, completeness.

**Business:** revenue/payment reconciliation, delivery dates, non-negative amounts.

### Data contracts

Для ключевых datasets YAML contract:

- owner;
- schema;
- types;
- nullability;
- SLA freshness;
- compatibility policy.

### Fault injection scenarios

- API 500/timeout/429;
- malformed CSV;
- missing S3 object;
- duplicate file;
- Kafka duplicate;
- ClickHouse unavailable;
- dbt test failure;
- Trino failure;
- late-arriving dimension/fact;
- schema evolution;
- partial batch.

### Acceptance criteria

Для каждого сценария описаны:

- expected behavior;
- retry policy;
- idempotency behavior;
- quarantine/DLQ behavior;
- alert;
- recovery runbook.

---

## Phase 11 — Iceberg maintenance и performance engineering

**Цель:** получить практический опыт эксплуатации lakehouse.

### Задачи

- snapshots/time travel;
- rollback после ошибочной загрузки;
- compact small files через optimize;
- expire snapshots;
- remove orphan files;
- partition evolution;
- schema evolution;
- benchmark partition pruning/predicate pushdown;
- собрать before/after metrics.

### Airflow DAG

`iceberg_maintenance` с отдельными задачами и безопасными retention settings.

### Acceptance criteria

- создан сценарий с большим количеством small files;
- optimize дает измеримое улучшение;
- rollback восстанавливает ошибочно измененный dataset;
- maintenance не удаляет актуальные данные.

---

## Phase 12 — Observability и lineage

**Цель:** сделать платформу диагностируемой.

### Prometheus/Grafana

Метрики:

- DAG success/failure/duration;
- dataset freshness;
- Kafka consumer lag;
- Trino query latency/errors/memory;
- ClickHouse query latency;
- PostgreSQL connections/WAL;
- MinIO storage/object metrics.

Dashboards:

1. Data Platform Health.
2. Pipeline SLA/Freshness.
3. Trino/ClickHouse Performance.
4. Kafka/CDC Health.

### OpenLineage + Marquez

Минимальная цепочка:

`PostgreSQL.orders -> bronze.orders -> silver.orders -> fact_orders -> mart_daily_sales -> ClickHouse`.

### Acceptance criteria

- можно определить, где упал pipeline и какой dataset устарел;
- lineage показывает upstream/downstream для ключевой витрины;
- runbook описывает диагностику трех типовых инцидентов.

---

## Phase 13 — GitHub Actions CI/CD v2

**Цель:** получить production-like delivery process.

### Workflow 1: `ci.yml`

На каждый PR:

- checkout;
- Python dependency cache;
- ruff;
- mypy;
- pytest unit;
- Airflow DAG import tests;
- dbt parse/compile;
- Docker Compose config validation.

### Workflow 2: `integration.yml`

На PR в `main` или вручную:

- поднять минимальный service subset на GitHub-hosted Linux runner;
- PostgreSQL + MinIO + catalog + Trino;
- выполнить smoke/integration tests;
- teardown always.

### Workflow 3: `docker-images.yml`

После merge в main/tag:

- build custom images;
- security scan;
- push versioned images в GHCR;
- запрещены mutable-only `latest` dependencies.

### Workflow 4: `release.yml`

- semantic tag/version;
- changelog;
- release notes;
- compose manifest versions.

### CD стратегия для WSL2

Основной вариант: локальный `make deploy`/`make upgrade` после успешного CI.

Опционально: self-hosted GitHub runner только для доверенных защищенных событий. Для публичного репозитория не разрешать fork PR выполнять job на локальном runner.

### Acceptance criteria

- PR нельзя merge без обязательных checks;
- integration test поднимает реальную минимальную инфраструктуру;
- images versioned и воспроизводимы;
- deployment procedure документирована.

---

## Phase 14 — Data Vault 2.0 mini-domain

**Цель:** получить знакомство с Data Vault без перегрузки основной архитектуры.

Реализовать для customer-order домена:

- `hub_customer`;
- `hub_order`;
- `link_customer_order`;
- `sat_customer_details`;
- `sat_order_details`.

Сравнить с Kimball:

- ingestion/history;
- complexity;
- query ergonomics;
- когда какой подход использовать.

Результат — отдельный ADR, а не замена основной Gold-модели.

---

## Phase 15 — Production simulation / capstone

**Цель:** проверить систему как единый продукт.

### Scenario A — обычный день

- OLTP continuous workload;
- CDC;
- API loads;
- supplier file;
- clickstream;
- dbt builds;
- ClickHouse publish;
- Superset dashboards.

### Scenario B — incident day

Одновременно:

- API дает 429;
- supplier CSV меняет schema;
- consumer перезапускается;
- ClickHouse недоступен 10 минут;
- один dbt business test падает.

Нужно восстановить систему без потери/дублирования данных.

### Scenario C — backfill

Пересчитать 30 дней исторических данных с контролем нагрузки и без дублирования.

### Итоговые метрики

- end-to-end freshness;
- pipeline success rate;
- records/sec ingestion;
- Trino query latency;
- ClickHouse query latency;
- storage size;
- small-file count;
- Spark processing time.

---

## Phase 16 — Airflow 2 -> Airflow 3 migration exercise

**Цель:** превратить обязательное использование Airflow 2 в дополнительный коммерчески полезный кейс.

- inventory deprecated APIs;
- provider compatibility audit;
- DAG compatibility tests;
- migration branch;
- обновление Docker images/config;
- regression run;
- ADR с breaking changes и rollback plan.

Airflow 2.11.2 остается baseline проекта, migration фиксируется отдельным release/branch.

---

## Phase 17 — GitLab CI migration (позже)

**Цель:** перенести существующую CI-модель, не переписывая инженерные правила.

Соответствие:

| GitHub Actions | GitLab CI |
|---|---|
| workflow | pipeline |
| job | job |
| GitHub-hosted runner | shared runner |
| self-hosted runner | GitLab Runner |
| repository secrets | CI/CD variables |
| GHCR | GitLab Container Registry |
| required checks | merge request pipeline rules |

Артефакт этапа: `.gitlab-ci.yml` с теми же lint/test/integration/build gates.

---

# 8. Порядок выполнения и ориентировочные итерации

Roadmap лучше выполнять не по календарю, а по завершенным вертикальным slices.

| Итерация | Результат |
|---|---|
| 1 | Repository + CI + standards |
| 2 | Core lakehouse работает end-to-end |
| 3 | OLTP generator + batch sources |
| 4 | Airflow ingestion |
| 5 | dbt Silver/Gold + Kimball |
| 6 | ClickHouse + Superset |
| 7 | CDC + Kafka |
| 8 | Spark clickstream |
| 9 | Data Quality + contracts + failure scenarios |
| 10 | Iceberg maintenance/performance |
| 11 | Observability + lineage |
| 12 | CI/CD hardening + GHCR |
| 13 | Data Vault mini-domain |
| 14 | Capstone production simulation |
| 15 | Airflow 3 migration |
| 16 | GitLab CI migration / Kubernetes optional |

Если работать по 8-12 часов в неделю, не привязывать качество к жесткому сроку: каждая итерация завершается только после acceptance criteria.

# 9. Приоритеты: что обязательно, а что опционально

## Must-have для portfolio v1

- PostgreSQL source;
- API + S3/file ingestion;
- Airflow 2.11.2;
- MinIO;
- Iceberg;
- Polaris;
- Trino;
- dbt Core;
- Kimball + SCD2;
- ClickHouse;
- Superset;
- GitHub Actions;
- unit/integration tests;
- idempotency/backfill;
- data quality;
- README + architecture diagram.

## Portfolio v2

- Kafka;
- Debezium;
- Spark;
- Iceberg maintenance;
- Prometheus/Grafana;
- OpenLineage/Marquez;
- performance benchmarks;
- fault injection.

## Later

- Data Vault mini-domain;
- Airflow 3 migration;
- GitLab CI;
- Kubernetes;
- Terraform только при появлении реальной cloud-инфраструктуры.

# 10. GitHub branching и delivery model

```text
main                 production-like stable state
  ^
  |
pull request
  ^
  |
feature/<issue>-...
```

Правила:

- изменения только через PR;
- минимум один green CI перед merge;
- Conventional Commits желательно;
- squash merge;
- GitHub Issues соответствуют roadmap tasks;
- milestone соответствует Phase;
- ADR нужен для значимого архитектурного решения.

Labels:

`area/airflow`, `area/dbt`, `area/trino`, `area/iceberg`, `area/kafka`, `area/spark`, `area/clickhouse`, `area/bi`, `area/infra`, `type/bug`, `type/feature`, `type/test`, `priority/p0..p2`.

# 11. Testing pyramid

```text
                  E2E
               /       \
        integration tests
       /                 \
 unit tests + dbt tests + contract tests
```

### Unit

- Python transforms/parsers;
- API clients;
- file validators;
- generators;
- Spark functions.

### Integration

- PostgreSQL -> ingestion;
- MinIO -> Iceberg -> Trino;
- dbt -> Trino;
- Gold -> ClickHouse;
- Debezium -> Kafka;
- Superset connectivity smoke test.

### E2E

`source mutation -> CDC/batch -> Iceberg -> dbt -> ClickHouse -> query KPI`.

# 12. Security baseline

- `.env` в `.gitignore`;
- `.env.example` без реальных secrets;
- отдельные service accounts;
- принцип least privilege;
- Superset user только SELECT;
- GitHub Actions secrets для registry/remote credentials;
- pinned action major versions и dependency pinning;
- network ports наружу публиковать только необходимые dev UI;
- MinIO/PostgreSQL/ClickHouse admin endpoints не публиковать без необходимости.

# 13. Documentation package

К концу проекта должны существовать:

- `README.md` — portfolio overview;
- `docs/architecture.md` — C4/container-level architecture;
- `docs/data-model.md` — facts/dimensions и grain;
- `docs/data-contracts.md`;
- `docs/sla-slo.md`;
- ADR directory;
- runbooks: failed API, Kafka lag, dbt failure, ClickHouse outage, bad supplier file;
- benchmark report;
- screenshots Superset/Grafana/Marquez;
- sample incident postmortem.

# 14. Финальные portfolio deliverables

1. Публичный GitHub repository.
2. Architecture diagram.
3. One-command local bootstrap для core environment.
4. Demonstration dataset generator.
5. 5 Superset dashboards.
6. Grafana monitoring dashboards.
7. Marquez lineage screenshot.
8. GitHub Actions green pipeline.
9. Performance comparison Trino/Iceberg vs ClickHouse.
10. Incident/backfill demonstration.
11. ADR collection.
12. Short demo video 5-10 минут.

# 15. Что считать успешным завершением проекта

Проект завершен не тогда, когда «все контейнеры запустились», а когда можно показать следующий сценарий:

1. Создается заказ в PostgreSQL.
2. Debezium фиксирует изменение через WAL.
3. Событие попадает в Kafka.
4. Bronze сохраняет immutable/raw representation.
5. Silver формирует очищенное текущее состояние.
6. dbt обновляет fact/dimension модели.
7. Gold формирует бизнес-витрину.
8. Publication process обновляет ClickHouse.
9. Superset показывает изменившийся KPI.
10. Airflow отображает orchestration state.
11. Grafana показывает health/freshness.
12. Marquez показывает lineage.
13. Повторный запуск не создает дублей.
14. Backfill и failure recovery описаны и воспроизводимы.

Если этот сценарий воспроизводится из чистого clone и документирован, проект уже можно уверенно использовать как серьезную portfolio-работу Data Engineer.

# 16. Первый backlog для AI-агента

Не начинать с Kafka/Spark. Первые задачи должны создать короткую рабочую вертикаль.

### Epic 0 — Repository foundation
- #1 Bootstrap repository and Python tooling.
- #2 Add Makefile and environment conventions.
- #3 Add GitHub Actions CI.
- #4 Add ADR and AGENTS.md conventions.

### Epic 1 — Core lakehouse
- #5 Add PostgreSQL source container.
- #6 Add MinIO and initialize buckets.
- #7 Add Polaris catalog.
- #8 Add Trino and Iceberg catalog.
- #9 Add core smoke test.

### Epic 2 — First end-to-end dataset
- #10 Create orders source schema and seed generator.
- #11 Implement batch extraction to landing.
- #12 Load orders into Iceberg Bronze.
- #13 Create dbt staging/Silver model.
- #14 Create `fact_orders` Gold model.
- #15 Publish `mart_daily_sales` to ClickHouse.
- #16 Connect Superset and create first Revenue dashboard.

Только после #16 переходить к Airflow orchestration, нескольким источникам, CDC и Spark.

# 17. Рекомендуемый prompt для запуска каждой задачи агентом

Использовать шаблон:

```text
Implement GitHub issue <N> for OmniRetail Data Platform.

Before changing code:
1. Read AGENTS.md, ROADMAP.md and relevant ADRs.
2. Inspect the existing implementation and tests.
3. Keep the scope limited to this issue.

Requirements:
- preserve existing behavior unless the issue explicitly changes it;
- use pinned/reproducible dependencies;
- do not hardcode secrets;
- add or update tests;
- add healthchecks/config validation where applicable;
- update documentation and .env.example if configuration changes;
- prefer idempotent operations;
- do not introduce a new technology without an ADR.

Before finishing:
- run the relevant lint/tests/smoke checks;
- report changed files;
- report commands executed and their result;
- report known limitations and the next logical issue.
```

---

## Технические решения, зафиксированные на старте

- BI: Apache Superset вместо DataLens.
- CI: GitHub + GitHub Actions вместо GitLab на первой версии.
- Container registry: GHCR.
- GitLab CI: отдельный поздний migration exercise.
- Airflow baseline: 2.11.2; затем отдельная миграция на Airflow 3.
- dbt Core: baseline 1.10.x, адаптер dbt-trino 1.9.x; диапазоны закреплены в `pyproject.toml`, точные версии — в `uv.lock`.
- Trino: требование 476+, конкретный release pin фиксируется в compose и Dependabot/Renovate обновляет через PR; initial target — 483.
- ClickHouse: serving layer, не master storage.
- Iceberg: source of truth.
- Superset -> ClickHouse: основной BI path; Superset -> Trino: ad-hoc path.
- Grafana: только observability, не основной BI.
- Spark: только для оправданных объемов/алгоритмов.
- Kubernetes: после полной Docker Compose версии.
