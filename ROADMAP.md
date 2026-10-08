# OmniRetail Data Platform — завершённый roadmap capstone

**Статус:** feature scope завершён на Phase 8. Репозиторий поддерживается как
maintenance-only portfolio project в соответствии с
[ADR 0009](docs/adr/0009-freeze-capstone-scope-at-phase-8.md).

> The project is a production-like educational capstone, not a production-ready
> platform.

Первоначальный roadmap предусматривал дальнейшее расширение платформы. Это
расширение остановлено осознанно: Phase 0–8 уже образуют самостоятельный
end-to-end capstone, а добавление новых подсистем снижало бы фокус на отдельных
этапах обработки данных. История исходного плана сохраняется в Git.

---

## 1. Цель и границы проекта

OmniRetail демонстрирует проектирование и эксплуатацию локальной аналитической
платформы для e-commerce на Windows 11 + WSL2 с 32 GB RAM.

Завершённый scope охватывает:

- детерминированный PostgreSQL OLTP source;
- batch ingestion из PostgreSQL, REST API и файлов/S3;
- PostgreSQL CDC через Debezium и Kafka;
- immutable/raw Bronze в Apache Iceberg;
- типизированное и delete-aware Silver-состояние;
- Kimball Gold-модель и бизнес-витрины через dbt + Trino;
- Airflow orchestration для batch и согласованного аналитического refresh;
- атомарную публикацию Gold-витрин в ClickHouse;
- BI-as-code в Apache Superset;
- unit, integration, dbt и DAG tests;
- документированные гарантии идемпотентности, restart safety и recovery.

Проект не заявляет production readiness. Известный технический долг и
ограничения остаются явно перечислены в [PROGRESS.md](PROGRESS.md).

### 1.1 Финальная граница scope

Phases 0–8 являются полным feature scope этого репозитория. Подробные
спецификации прежних Phase 9–18 удалены из активного roadmap и не считаются
отложенным backlog OmniRetail.

Разрешённые после завершения изменения:

- документация;
- исправления ошибок;
- security и dependency maintenance;
- устранение технического долга в пределах реализованной архитектуры.

Новая подсистема, новый feature domain или возврат исключённой инициативы
требуют ADR, явно superseding ADR 0009.

## 2. Бизнес-сценарий и реализованные результаты

**OmniRetail** — условная e-commerce компания с интернет-магазином, клиентами,
заказами, платежами, доставкой, рекламными кампаниями и внешними поставщиками.

Реализованная платформа позволяет анализировать:

1. GMV, revenue, margin, количество заказов и AOV по времени.
2. Продажи и маржу по категориям и регионам.
3. Customer LTV, new/repeat composition и клиентские сегменты.
4. Delivery performance, transit time и status mix по перевозчикам.
5. Marketing spend, impressions, clicks, CTR, CPC, CPM и budget utilization.
6. Согласованность заказов и платежей.
7. Изменения и удаления customers, orders и payments, доставленные через CDC.

Clickstream funnel, conversion attribution, CAC и ROAS не входят в реализованный
scope: у платформы нет необходимого clickstream/attribution source.

## 3. Реализованная архитектура

```text
SOURCES

PostgreSQL OLTP
  ├── batch snapshots ── MinIO archive ── Bronze loader ──┐
  └── WAL ── Debezium ── Kafka ── CDC consumer ──────────┤
                                                          ├──> Iceberg Bronze
Mock REST APIs ── Airflow/Python ── MinIO archive ── Bronze loader ──────┤
Supplier files ── Airflow/Python ── MinIO archive ── Bronze loader ──────┘

Iceberg Bronze
      │
      │  Trino + dbt Core
      ▼
Typed/delete-aware Silver
      │
      ▼
Kimball Gold facts/dimensions
      │
      ▼
Analytics marts in Iceberg
      │
      │  atomic full-snapshot publication
      ▼
ClickHouse serving layer
      │
      ▼
Apache Superset dashboards

ORCHESTRATION: Airflow 2.11.2
CATALOG: Apache Polaris (Iceberg REST catalog)
OBJECT STORAGE: MinIO
CI: GitHub Actions
RUNTIME: Docker Compose profiles
```

Airflow координирует batch ingestion и аналитический refresh. Перед запуском
dbt он фиксирует здоровую и стабильную exclusive Kafka-offset boundary, после
успешных моделей и тестов атомарно перепубликует ClickHouse. Transformation SQL
остаётся в dbt, а не в DAG-коде.

### 3.1 Ответственность компонентов

| Компонент | Реализованная роль |
| --- | --- |
| PostgreSQL | OLTP source; отдельные metadata databases для сервисов |
| Python ingestion | API, file и PostgreSQL snapshot extraction; Bronze loading |
| Debezium | Захват изменений customers, orders и payments из PostgreSQL WAL |
| Kafka | Persistent transport для CDC events |
| CDC consumer | Restart-safe insert-only доставка raw CDC в Iceberg Bronze |
| MinIO | S3-compatible landing, archive, rejected и lakehouse storage |
| Iceberg | Аналитический source of truth: Bronze, Silver, Gold и marts |
| Polaris | Iceberg REST catalog |
| Trino | SQL compute над Iceberg и ad-hoc query path для Superset |
| dbt Core | Типизация, deduplication, SCD2, facts, dimensions, marts и tests |
| Airflow | Orchestration, retries, dataset triggers и stable-boundary refresh |
| ClickHouse | Производный low-latency serving layer, rebuildable из Iceberg Gold |
| Superset | BI dashboards через ClickHouse; ad-hoc exploration через Trino |
| GitHub Actions | Lint, unit tests, dbt parse, Compose validation и DAG tests |

### 3.2 Архитектурные инварианты

- Iceberg остаётся аналитическим source of truth.
- ClickHouse не содержит единственную копию бизнес-логики или данных и полностью
  пересобирается из Iceberg Gold.
- Superset использует ClickHouse для dashboard workload и Trino для ad-hoc
  exploration.
- Airflow оркестрирует, но не хранит transformation SQL.
- CDC Bronze сохраняет raw event semantics и transport/source coordinates.
- Повторная доставка и повторный запуск не должны искажать итоговое состояние.
- Конфигурация внешняя; секреты не хранятся в Git.

## 4. Ограничения среды и Compose profiles

Целевая машина:

```text
Windows 11 + WSL2
Physical RAM: 32 GB
Recommended WSL2 memory: 24 GB
Recommended processors: 6
Recommended swap: 8 GB
```

Рекомендуемая конфигурация WSL2:

```ini
[wsl2]
memory=24GB
processors=6
swap=8GB
```

Реализованные Compose profiles:

| Profile | Состав и назначение |
| --- | --- |
| `core` | PostgreSQL, MinIO, Polaris, Trino, deterministic mock API |
| `streaming` | Kafka, Debezium Connect, init jobs, CDC consumer |
| `orchestration` | Airflow webserver/scheduler и metadata PostgreSQL |
| `bi` | ClickHouse, Superset и их init/metadata services |

Профили запускаются по необходимости; весь стек не обязан постоянно работать
одновременно. Persistent volumes и destructive reset-команды должны оставаться
явными. Все host ports привязаны к `127.0.0.1`.

## 5. Структура репозитория

```text
omni-retail-data-platform/
├── .github/
│   └── workflows/             # GitHub Actions CI
├── airflow/
│   ├── dags/                  # ingestion, Bronze и transform DAGs
│   ├── include/               # shared datasets, policy и runners
│   └── tests/                 # DAG import/structure tests
├── clickhouse/
│   └── migrations/            # versioned serving DDL and grants
├── dbt/
│   ├── macros/
│   ├── models/
│   │   ├── staging/
│   │   ├── intermediate/
│   │   ├── core/
│   │   └── marts/
│   └── tests/                 # singular business/reconciliation tests
├── infrastructure/
│   ├── airflow/               # custom Airflow image
│   ├── cdc/                   # non-root CDC consumer image
│   ├── mock_api/              # deterministic source simulator
│   ├── superset/              # custom Superset image
│   └── scripts/               # bootstrap, recovery and smoke scripts
├── postgres/
│   └── init/                  # idempotent OLTP DDL
├── src/omni_retail/
│   ├── generators/            # OLTP and vendor-file generators
│   ├── ingestion/             # API, files, snapshots, Bronze loader
│   ├── lakehouse/             # Iceberg Bronze operations
│   ├── serving/               # ClickHouse publisher/benchmark
│   └── streaming/             # CDC consumer and contracts
├── superset/
│   └── assets/                # sanitized dashboards/datasets as code
├── tests/
│   ├── fakes/
│   ├── integration/
│   └── unit/
├── trino/etc/                 # Trino and Iceberg catalog configuration
├── docs/                      # ADRs, runbooks, model, contracts, evidence
├── docker-compose.yml
├── Makefile
├── pyproject.toml
├── uv.lock
├── .env.example
├── AGENTS.md
├── PROGRESS.md
├── ROADMAP.md
└── README.md
```

Новые top-level каталоги для исключённых инициатив не создаются без нового ADR,
который пересматривает границу scope.

## 6. Зафиксированные технические решения

- Python baseline: 3.12; dependency manager: `uv`; точные Python dependency
  versions фиксирует `uv.lock`.
- Runtime: Docker Compose с pinned images, без floating `latest` tags.
- Airflow baseline: 2.11.2 с LocalExecutor и отдельной metadata database.
- dbt Core baseline: 1.10.x; dbt-trino: 1.9.x.
- Trino pinned release: 483.
- Iceberg REST catalog: Apache Polaris.
- Object storage: MinIO.
- CDC: PostgreSQL logical replication → Debezium → Kafka → Iceberg consumer.
- Analytical model: Kimball-style Gold; customer SCD Type 2.
- BI: Superset → ClickHouse для dashboards; Superset → Trino для ad-hoc SQL.
- CI: GitHub Actions.
- ClickHouse — serving copy, не source of truth.

Точные container tags и dependency versions принадлежат
`docker-compose.yml`, Dockerfiles и `uv.lock`; roadmap фиксирует
ответственность компонентов, а не дублирует каждый patch pin.

## 7. Завершённые фазы

| Phase | Scope | Status |
| --- | --- | --- |
| 0 | Bootstrap и engineering standards | done |
| 1 | PostgreSQL, MinIO, Polaris, Trino и Iceberg core | done |
| 2 | OLTP model и deterministic generator | done |
| 3 | Batch ingestion: REST APIs, files/S3 и PostgreSQL snapshots | done |
| 4 | Airflow orchestration | done |
| 5 | dbt + Trino: Bronze → Silver → Gold | done |
| 6 | ClickHouse serving layer | done |
| 7 | Apache Superset BI | done |
| 8 | PostgreSQL CDC: Debezium → Kafka → Iceberg | done |

### Phase 0 — Bootstrap и инженерные стандарты

**Результат:** воспроизводимый Python repository с контролируемыми изменениями.

Реализовано:

- `pyproject.toml`, Python 3.12 и `uv.lock`;
- ruff, mypy, pytest и pre-commit;
- Makefile с безопасными entry points;
- `.env.example` и правила secrets management;
- GitHub Actions CI;
- ADR mechanism и agent instructions.

Acceptance criteria:

- documented setup создаёт окружение через `uv sync`;
- `make lint` и `make test` доступны локально;
- CI запускает lint/tests и configuration checks;
- repository не содержит committed secrets.

### Phase 1 — Core lakehouse

**Результат:** минимальный persistent Iceberg lakehouse на локальном Compose.

Реализовано:

- PostgreSQL OLTP service;
- MinIO buckets `landing`, `lakehouse`, `archive`, `rejected`;
- Polaris REST catalog и metadata PostgreSQL;
- Trino с Iceberg catalog через Polaris;
- healthchecks, init jobs и persistent volumes;
- `make smoke-core` для Trino → Polaris → Iceberg → MinIO.

Acceptance criteria:

- core services поднимаются через `make up` и readiness checks;
- Trino создаёт, записывает и читает Iceberg table;
- данные сохраняются после restart;
- credentials приходят из `.env`, а не из Git.

### Phase 2 — OLTP-модель и генератор данных

**Результат:** реалистичный, детерминированный e-commerce source.

Таблицы:

- `customers`;
- `products`;
- `categories`;
- `orders`;
- `order_items`;
- `payments`;
- `shipments`.

Реализовано:

- PK/FK/CHECK constraints и audit timestamps;
- deterministic initial load с фиксированным seed;
- controlled mutation workload с inserts, updates и hard deletes;
- order/payment/shipment state transitions;
- защита от неявного destructive reseed.

Acceptance criteria:

- одинаковый seed воспроизводит одинаковый dataset;
- source rows проходят constraints;
- update/delete workload можно запустить отдельно;
- source grain и metrics документированы в `docs/data-model.md`.

### Phase 3 — Batch ingestion

**Результат:** идемпотентный raw ingestion для типовых batch sources.

Реализовано:

- REST sources: FX rates, marketing campaigns и delivery status;
- deterministic mock API с pagination и fault injection;
- supplier CSV, partner JSON, historical-orders Parquet и XLSX edge case;
- PostgreSQL snapshots с keyset pagination и durable watermarks;
- MinIO flow `landing → processing → archive | rejected`;
- checksums, manifests, quarantine и logical-date backfill;
- Bronze loading для API, PostgreSQL snapshot archive и всех четырёх
  manifest-backed supplier file formats.

Acceptance criteria:

- повторный batch не создаёт дубли;
- malformed file/row попадает в `rejected` с machine-readable reason;
- API 429/500/timeout использует bounded retry policy;
- backfill определяется explicit logical dates;
- interrupted snapshot/load можно безопасно повторить.

Supplier files загружаются из immutable raw archive в четыре типизированные
Iceberg Bronze-таблицы. Загрузка разрешается только canonical completed
manifest-ом, сверяет checksum и accepted/rejected row counts и безопасно
возобновляется по стабильным координатам source object + row position. История
snapshots четырнадцати batch Bronze-таблиц ограничена maintenance-политикой:
30 дней и минимум 10 последних ancestors, с safety floors 7 дней / 2 snapshots.

### Phase 4 — Airflow orchestration

**Результат:** ingestion и lakehouse workflow координируются Airflow без
дублирования transformation logic.

Реализованные DAGs:

- `ingest_fx_api`;
- `ingest_marketing_api`;
- `ingest_delivery_api`;
- `ingest_supplier_files`;
- `ingest_postgres_snapshot`;
- `load_bronze`;
- `maintain_iceberg_snapshots`;
- `transform_lakehouse`.

Acceptance criteria:

- DAGs успешно импортируются и проходят structure tests;
- retry/backoff, timeout, pools и `max_active_runs` заданы явно;
- logical date используется вместо wall-clock paths;
- dataset-triggered Bronze и transform paths не дублируют бизнес-SQL;
- независимые ingestion tasks изолируют сбои источников.

### Phase 5 — dbt + Trino: Bronze → Silver → Gold

**Результат:** аналитическая модель над Iceberg с typed CDC state и бизнес
reconciliation.

Реализованные слои:

- staging — casting, naming и source normalization;
- intermediate — deduplication, joins, FX normalization и CDC state derivation;
- core — facts, dimensions и customer SCD2;
- marts — dashboard-ready aggregates.

Ключевые Gold datasets:

- `dim_customer`, `dim_product`, `dim_date`, `dim_campaign`;
- `fact_orders`, `fact_order_items`, `fact_payments`, `fact_shipments`;
- `mart_daily_sales`;
- `mart_customer_ltv`;
- `mart_marketing_roi`;
- `mart_delivery_performance`.

Acceptance criteria:

- dbt parse/build/test path определён и воспроизводим;
- CDC-backed customers/orders/payments обрабатывают create/update/delete;
- SCD2 intervals не пересекаются и имеют одну current row;
- Gold keys соответствуют текущему CDC state;
- orders/payments и mart totals проходят business reconciliation;
- model grain, contracts и lineage на уровне dbt задокументированы.

### Phase 6 — ClickHouse serving layer

**Результат:** rebuildable low-latency serving copy для BI.

Реализовано:

- versioned migrations и schema ledger;
- mart-specific MergeTree tables;
- least-privilege `omni_publisher` и read-only `superset_reader`;
- full-snapshot staging load и atomic `EXCHANGE TABLES` publication;
- single-mart и all-mart rebuild из Iceberg Gold;
- Trino/Iceberg vs ClickHouse benchmark.

Acceptance criteria:

- repeated publication не создаёт дубли;
- reader не видит partial refresh;
- ClickHouse полностью пересобирается из Iceberg Gold;
- BI account не имеет write permissions;
- outage recovery документирован в runbook.

### Phase 7 — Apache Superset

**Результат:** воспроизводимый BI layer и dashboards as code.

Реализовано:

- Superset → ClickHouse primary connection;
- Superset → Trino ad-hoc connection;
- sanitized datasets и dashboard bundles в repository;
- idempotent bootstrap через Compose init job;
- Sales, Executive, Customer и Marketing dashboards.

Acceptance criteria:

- connection использует Docker-network hostnames и least-privilege accounts;
- metadata/assets восстанавливаются из clean repository state плюс `.env`;
- integration test проверяет bootstrap, ClickHouse canary и Trino SQL Lab path;
- четыре dashboard screenshots сохранены в `docs/screenshots/` и показаны в
  основном README как обязательный capstone artifact.

### Phase 8 — CDC: PostgreSQL → Debezium → Kafka → Iceberg

**Результат:** restart-safe CDC для ключевых OLTP entities и согласованный
аналитический refresh.

Реализовано:

- PostgreSQL logical replication initialization;
- pinned Kafka в single-node KRaft mode;
- Debezium PostgreSQL connector;
- topics для customers, orders и payments;
- persistent Kafka/Connect state;
- route-aware CDC contract и insert-only Iceberg Bronze table;
- offset commit только после successful Iceberg MERGE;
- typed/delete-aware current state по PostgreSQL LSN и Kafka offsets;
- customer SCD2 lifecycle, hard-delete handling и recreate semantics;
- stable lag-zero boundary для dbt;
- atomic ClickHouse republish только после successful dbt graph/tests.

Acceptance criteria:

- source changes появляются в Bronze без повторного full scan;
- initial Debezium snapshot формирует baseline;
- create/update/delete корректно меняют analytical state;
- duplicate delivery не искажает финальный результат;
- consumer restart продолжает работу с committed offsets;
- out-of-order event time не заменяет source ordering;
- refresh не смешивает разные Kafka frontiers;
- recovery и destructive reset semantics описаны в runbook.

## 8. Capstone completion scenario

Проект считается завершённым в выбранном scope, когда воспроизводимо
демонстрируется следующий путь:

1. PostgreSQL содержит детерминированный OLTP baseline.
2. Batch paths сохраняют raw API и PostgreSQL snapshot payloads в MinIO archive
   и загружают поддерживаемые sources в Iceberg Bronze.
3. Создание, изменение или удаление customer/order/payment фиксируется Debezium
   через PostgreSQL WAL и попадает в Kafka.
4. Restart-safe consumer сохраняет immutable CDC event в Iceberg Bronze и только
   после успешной записи подтверждает offset.
5. Airflow фиксирует здоровую стабильную Kafka boundary.
6. dbt строит typed/delete-aware Silver, Gold facts/dimensions и четыре marts;
   critical dbt tests и reconciliation проходят.
7. Publication process загружает полный mart snapshot в staging ClickHouse table
   и атомарно переключает serving table.
8. Superset читает обновлённые KPI через read-only ClickHouse connection.
9. Повторный логический запуск не создаёт дубли и приводит систему к тому же
   результату.
10. Основные failure/recovery procedures доступны в runbooks.

Dashboard screenshots завершают визуальное portfolio evidence, но не заменяют
автоматические проверки данных и инфраструктуры.

## 9. Testing и validation model

```text
                  E2E acceptance paths
                /                      \
       live integration tests      dbt business tests
              /                           \
       unit tests                 contracts + reconciliation
```

Минимальные validation entry points:

```bash
make lint
make test
make dbt-parse
make airflow-test

docker compose config
make smoke-core       # live core required
make integration      # live profiles required for their tests
```

Правила:

- команда считается прошедшей только после фактического successful run;
- unit tests не зависят от public APIs или containers;
- integration tests используют disposable schemas и rollback journals;
- deterministic fixtures используют fixed seeds и timestamps;
- destructive tests не должны сбрасывать shared production-like schemas;
- новые проверки добавляются только для delivered scope или его maintenance.

## 10. Известные ограничения и технический долг

Текущий status и полный реестр принадлежат [PROGRESS.md](PROGRESS.md). На момент
фиксации capstone существенны следующие ограничения:

- clean-host bootstrap зависит от восстановления воспроизводимой поставки
  pinned MinIO images (TD-001);
- batch Bronze snapshot history имеет bounded expiration; CDC microbatches всё
  ещё требуют CDC-specific compaction/retention при длительной эксплуатации
  (TD-006);
- dbt contracts документированы и тестируются, но не полностью enforced
  adapter-ом (TD-004);
- отдельные generic-test definitions сохраняют dbt v2 migration debt (TD-005).

Capstone status не закрывает и не скрывает эти пункты. Технический долг можно
устранять как maintenance, если изменение не добавляет новую platform subsystem
и не восстанавливает удалённую фазу как инициативу.

## 11. Maintenance и delivery policy

Основная ветка содержит стабильное состояние capstone:

```text
main
  ^
  |
pull request
  ^
  |
fix/<issue>-... | docs/<issue>-... | chore/<issue>-...
```

Требования к изменениям:

- focused PR и green применимые CI checks;
- pinned/reproducible dependencies;
- tests вместе с исправлением поведения;
- documentation update при изменении usage или operations;
- отсутствие новых secrets;
- ADR до изменения архитектурной границы;
- сохранение Iceberg source-of-truth и ClickHouse rebuildability;
- честное обновление `PROGRESS.md` для debt/status changes.

Remote issues, существующие только для реализации удалённого future scope,
закрываются как `not planned` со ссылкой на ADR 0009; соответствующие milestones
закрываются или выводятся из использования.

## 12. Документационный пакет

| Документ | Ответственность |
| --- | --- |
| `README.md` | Portfolio overview, implemented architecture, quick start и evidence |
| `docs/learning-path.md` | Overview, guided code tour, Core demo и Full demo |
| `ROADMAP.md` | Финальный scope, completed phases и acceptance criteria |
| `PROGRESS.md` | Текущее состояние и открытый technical debt |
| `docs/adr/` | Архитектурные решения и история изменения scope |
| `docs/data-model.md` | Grain, facts, dimensions и source/model semantics |
| `docs/data-contracts.md` | Source и modeled-data contracts |
| `docs/runbooks/README.md` | Индекс recovery и типовых operational procedures |
| `docs/benchmarks/` | Воспроизводимые performance comparisons |
| `docs/screenshots/` | Визуальные dashboard artifacts и capture procedure |
| `AGENTS.md`, `docs/agent/` | Нормативные правила для coding agents |

Audit trail исходного расширенного roadmap сохраняется в Git history. Активная
документация описывает только текущее состояние и выбранную границу capstone.

## 13. Отдельные проекты

Продолжение работы по отдельным темам обработки данных намеренно вынесено в
самостоятельные репозитории этого профиля. Они имеют собственные границы,
архитектурные решения и критерии завершения и не являются незавершёнными фазами
OmniRetail.
