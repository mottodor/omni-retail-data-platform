# Phase 5 — dbt + Trino: Bronze → Silver → Gold (Design Spec)

- **Дата:** 2026-09-18
- **Статус:** Approved (design review пройден в диалоге)
- **Ветка:** `feature/phase5-dbt-lakehouse` (от `main` @ 92f93ce, Phase 4 смержена через PR #4)
- **Связанные документы:** `ROADMAP.md` (Phase 5), `AGENTS.md` (§14, §20–22, §24, §30, §43), `docs/adr/0001-project-architecture.md`, `docs/superpowers/specs/2026-09-11-phase3-batch-ingestion-design.md`, `docs/superpowers/specs/2026-09-11-phase4-airflow-orchestration-design.md`

---

## 1. Контекст и цель

Phase 0–4 завершены: raw-данные лежат в MinIO (`archive/postgres/<table>/…/data.parquet` — инкрементальные PG-снапшоты с watermark; `archive/api/<source>/<yyyymmdd>/page_XXXX.json` — сырые страницы API; манифесты в `archive/_manifests/`), Trino 483 + Polaris + Iceberg работают, Airflow оркестрирует 5 ingestion DAG-ов.

**Bronze/Silver/Gold в Iceberg и dbt отсутствуют.** Phase 5 строит аналитическое ядро платформы:

1. загрузка raw → Iceberg **Bronze** (питает всё downstream);
2. dbt-проект на dbt-trino: **staging → intermediate → core → marts**;
3. Kimball-модель: 4 измерения (`dim_customer` SCD2, `dim_product`, `dim_date`, `dim_campaign`), 4 факта, 4 бизнес-витрины;
4. качество: встроенные dbt-тесты + custom business tests (включая orders ↔ payments reconciliation);
5. закрывается отложенный в Phase 4 dataset-паттерн `lakehouse_bronze_ready`.

## 2. Зафиксированные решения (design review)

| Решение | Выбор | Отвергнутые альтернативы |
|---|---|---|
| Механизм загрузки Bronze | Python-модуль `src/omni_retail/lakehouse/bronze/`: читает raw из `archive` (parquet через pyarrow из S3-байт, JSON-страницы через stdlib) и пишет через Trino SQL (batched `INSERT`); идемпотентность — `DELETE` партиции + `INSERT` per (таблица, logical date) | hive-каталог поверх MinIO (нужен metastore ⇒ новый сервис); `EXECUTE add_files` (Iceberg-таблица ссылается на raw-файлы: lifecycle-связь с archive + проблема object-overwrite при retry того же дня); pyiceberg (новая зависимость, версионно-чувствительные overwrite-семантики) |
| SCD2 `dim_customer` | явная модель из append-only истории Bronze (`_batch_date`-поток версий): surrogate key, `valid_from`/`valid_to`/`is_current`; полностью пересобираема и детерминирована (AGENTS §30, §38) | dbt snapshots: stateful-таблица `snapshot_*`, муторный исторический rebuild |
| Runtime dbt | `dbt-core>=1.10,<1.11` + `dbt-trino>=1.9,<1.10` (pinned через `uv.lock`); **основные** зависимости (не dev) — чтобы попасть в Airflow-образ через `uv export --no-dev` (слайс 3); `dbt/` — верхнеуровневый каталог по ROADMAP §5 | отдельный dbt-контейнер (лишний сервис); dbt только локально (не оркестрируется) |
| Схемы в Iceberg | `bronze` (внешняя для dbt: создаётся/пишется Python-загрузчиком, dbt читает как `sources`), `silver` (staging + intermediate), `gold` (core: dims/facts), `analytics` (marts) — нейминг AGENTS §43.1 | всё в одной схеме; staging в bronze (смешение ответственности) |
| Источники Phase 5 | OLTP-снапшоты (7 таблиц) + 3 API (fx-rates, marketing, delivery) | 4 файловых источника — вне скоупа: не кормят целевые марты (§47); Bronze для них — follow-up issue |
| `mart_marketing_roi` | честные метрики кампаний (spend/бюджет, CTR, CPC, CPM): атрибуции выручки по кампаниям нет — кампании не связаны с заказами; ROAS-атрибуция появится с clickstream (Phase 9). Ограничение документируется в модели | фейковый ROAS через total revenue (нечестно) |
| `fact_shipments` vs delivery API | факт строится из OLTP `shipments` (реальные FK); delivery-API фид (`order_id` синтетический `ORD-…`, к OLTP не присоединяется) независимо питает `mart_delivery_performance` (логистический партнёр-витрина: carrier/статусы/сроки) | попытка join delivery API с orders (невозможно по данным) |

## 3. Архитектура

```text
MinIO archive                                Iceberg (Polaris catalog, Trino 483)
  postgres/<t>/<yyyy>/<mm>/<dd>/data.parquet ─┐
  api/<source>/<yyyymmdd>/page_XXXX.json      ─┴→ bronze (Python-загрузчик; партиция _batch_date)
                                                     │  dbt sources
                                                     ▼
                                             silver: stg_* (10) + int_* (текущее состояние, dedup)
                                                     │
                                                     ▼
                                             gold: dim_* (4, SCD2 у customer) + fact_* (4)
                                                     │
                                                     ▼
                                             analytics: mart_* (4)

Airflow (слайс 3): ingestion DAG-и → dataset `lakehouse://bronze` → DAG transform_lakehouse:
  load_bronze (run-all за logical date) → dbt build (запечён в Airflow-образ)
```

- Загрузчик и dbt локально ходят на `127.0.0.1:8080`; в Airflow — `trino:8080` (env).
- Схемы `silver`/`gold`/`analytics` создаёт dbt; `bronze` — загрузчик (`CREATE SCHEMA IF NOT EXISTS` через Trino; привилегии уже есть — smoke-core создаёт `iceberg.demo` тем же принципалом).

## 4. Bronze слой

**Таблицы (10):** `orders`, `order_items`, `customers`, `products`, `categories`, `payments`, `shipments` (OLTP-снапшоты) + `fx_rates`, `campaigns`, `deliveries` (развернутые API-страницы).

- Декларативные `BronzeTableSpec` (имя, колонки+типы Trino, партиция) — по образцу `postgres_snapshot/tables.py`; типы повторяют source (bigint/varchar/boolean/decimal/timestamp(6) with time zone/date).
- Служебные колонки каждой bronze-таблицы: `_batch_id varchar` (`postgres-<table>-<yyyymmdd>` / `api-<source>-<yyyymmdd>` — детерминирован), `_batch_date date` (логическая дата; **партиционирование**), `_source_object varchar` (ключ raw-объекта), `_ingested_at timestamp with time zone` (wall-clock загрузки).
- Ридеры: parquet → `pyarrow` из байтов объекта (контроль набора колонок против spec — schema drift = явная ошибка); API JSON → развертка envelope (`rates`/`campaigns`/`deliveries`) постранично, страница целиком = один `_source_object`.
- **Идемпотентность:** `load(source, date)` = `DELETE FROM bronze.<t> WHERE _batch_date = date` → batched `INSERT` (строки ~500/стейтмент); повтор/ретрай того же дня перезаписывает партицию, дублей нет. Проверка row_count против манифеста `_manifests/<source>/<batch_id>.json` (расхождение — ошибка).
- Пустой batch (нет объектов за дату) — no-op c warning (не ошибка: инкрементальные окна бывают пустыми).
- CLI: `python -m omni_retail.lakehouse.bronze run --source <t> --date <d>` / `run-all --date <d>`; структурные логи (source, batch_id, row_count).

## 5. dbt-проект

```text
dbt/
  dbt_project.yml      каталог iceberg; слои → схемы через custom generate_schema_name
  profiles.yml         env-driven (TRINO_HOST/PORT/USER, DBT_SCHEMA), коммитится (без секретов)
  models/
    staging/           stg_<table>: renaming, cast, минимальная очистка; sources.yml
    intermediate/      int_<entity>: dedup по pk (последняя версия по _ingested_at/_batch_date),
                       текущее состояние сущностей; fx-курс по дням; снапшот кампаний
    core/              dim_date (SQL-генерация календаря), dim_product(+category),
                       dim_customer (SCD2), dim_campaign; fact_orders, fact_order_items,
                       fact_payments, fact_shipments
    marts/             mart_daily_sales (+нормализация валют → EUR по fx), mart_customer_ltv,
                       mart_marketing_roi, mart_delivery_performance
  tests/               singular: orders↔payments reconciliation, SCD2-интервалы,
                       неотрицательность сумм, временная упорядоченность
```

- Материализации: staging — view; intermediate — view/ephemeral; core, marts — table (полный пересчёт = детерминизм; инкрементальность не требуется при laptop-объёмах, Iceberg maintenance — Phase 11).
- Каждый core/mart-модель — контракт в `schema.yml`: purpose, grain, PK/uniqueness, меры, upstream (AGENTS §20.5).
- Source freshness: `loaded_at_field: _ingested_at` (warn 48h / error 7d).
- dbt docs (`dbt docs generate`) — lineage виден (acceptance ROADMAP).

## 6. Политики ошибок (явные)

| Сценарий | Поведение |
|---|---|
| Trino/MinIO недоступны | ошибка с контекстом; в Airflow — task retries (existing policy) |
| schema drift raw-объекта | явная ошибка ридера (имена колонок vs spec), партиция не трогается |
| расхождение row_count с манифестом | ошибка загрузки, партиция остаётся консистентной (DELETE+INSERT в одной logical-операции дня) |
| пустой batch | no-op, warning |
| падение между DELETE и INSERT | retry дня перезапускает пару целиком — консистентно |
| dbt test failure | `dbt build` красный, DAG падает; данные детерминированно пересчитываются (карантин не нужен) |

## 7. Зависимости

Новые **основные** pinned-зависимости: `dbt-core>=1.10,<1.11`, `dbt-trino>=1.9,<1.10` (клиент `trino` приходит транзитивно; переиспользуется загрузчиком Bronze). Риск пересечения с constraints Airflow 2.11.2 оценивается сборкой образа в слайсе 3; откат — отдельный dbt-образ (документируется в ADR, если потребуется).

## 8. Конфигурация (→ `.env.example`, guard-тест)

- `TRINO_HOST` (default `127.0.0.1`; compose/Airflow: `trino`), `TRINO_PORT` (8080), `TRINO_CATALOG` (`iceberg`), `TRINO_USER` (`omni`).

## 9. Тестирование

- **Unit (dev venv):** ридеры (fixtures: parquet-байты, JSON-страницы), SQL-литералы/батчинг, loader на fake-клиенте (DELETE→INSERT порядок, идемпотентность re-run), specs-валидация, CLI.
- **Guard-тесты compose/env:** покрытие новых переменных, отсутствие секретов в profiles.yml.
- **CI:** dbt `parse`/`compile` без живого стека (новый job-step) + существующие ruff/mypy/pytest.
- **Integration (live core, `OMNI_INTEGRATION=1`):** слайс 1 — bronze load + повторный run без дублей + `dbt run` staging; слайс 2 — полный `dbt build` с тестами на фиксированных данных генератора; слайс 3 — DAG `transform_lakehouse` через `airflow dags test`.

## 10. Слайсы

| Слайс | Содержимое |
|---|---|
| 1 | Спека (этот документ) + зависимости + dbt-скелет (project/profiles/staging OLTP 7 таблиц + sources.yml) + модуль `lakehouse/bronze` + unit-тесты + Make-таргеты (`bronze-load`, `dbt-parse/build/test`) + CI dbt-parse + guard-тесты + integration-тест bronze + `.env.example` |
| 2 | staging API (3 таблицы) + intermediate + core Kimball (SCD2) + контракты моделей + встроенные и singular-тесты (reconciliation) + `docs/data-model.md` |
| 3 | marts (4) + Airflow: dataset `lakehouse://bronze`, DAG `transform_lakehouse`, dbt в Airflow-образе + dbt docs + README + финальная integration-проверка |

## 11. Out of scope

- Bronze для 4 файловых источников (follow-up issue);
- ClickHouse-публикация и Superset (Phase 6–7);
- инкрементальные материализации dbt и Iceberg maintenance/optimize (Phase 11);
- инкрементальные факты по событиям CDC (Phase 8);
- ROAS-атрибуция выручки (Phase 9, clickstream);
- dbt-контракты как enforcement (если dbt-trino не поддержит) — остаются YAML-документацией + тестами.

## 12. Соответствие acceptance criteria ROADMAP (Phase 5)

| Критерий | Чем закрывается |
|---|---|
| `dbt build` проходит | слайс 2, integration-тест полного build |
| SCD2 корректно хранит историю | явная SCD2-модель + тесты (нет пересечений, один current, история сохраняется) |
| marts имеют documentation/description | контракты моделей в `schema.yml` (§5) |
| lineage виден хотя бы в dbt docs | `dbt docs generate` (слайс 3) |
| бизнес reconciliation orders vs payments | singular-тест (слайс 2) |
| unique/not_null/relationships/accepted_values/freshness | встроенные тесты + source freshness (§5) |
