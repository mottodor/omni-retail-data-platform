# Agent guide — Airflow orchestration

Part of the repository agent rules, split out of the monolithic `AGENTS.md`.

Read this guide before working on: Airflow DAGs, task design, scheduling,
retries, backfills, or Airflow configuration.

Precedence: explicit user request > `AGENTS.md` core > accepted ADRs > this
guide > `ROADMAP.md` > existing implementation conventions. On conflict with
the `AGENTS.md` core, the core wins.

Contains original AGENTS.md section 19. Section numbers are preserved so
existing references of the form "AGENTS §N" keep resolving.

---

## 19. Airflow rules

Airflow baseline is Apache Airflow 2.11.2.

### 19.1 Airflow responsibility

Airflow coordinates work.

Airflow DAG files must not become transformation code repositories.

Do not embed large business SQL transformations inside DAGs when the SQL belongs in dbt.

Airflow should orchestrate:

- extraction;
- loading;
- dbt execution;
- publishing;
- maintenance;
- data quality;
- recovery workflows.

### 19.2 DAG design

Prefer:

- TaskFlow API where appropriate;
- small tasks;
- explicit dependencies;
- isolated failure domains;
- pools for constrained external systems;
- timeouts;
- retry policy;
- logical date/data interval;
- parameterized backfills.

Avoid:

- mega-DAGs;
- one task doing ingestion + transformation + publication;
- network calls at DAG import time;
- database connections at DAG import time;
- dynamic behavior that breaks deterministic DAG parsing.

### 19.3 Airflow configuration

Use:

- environment variables or proper config;
- Airflow Variables only for non-secret runtime configuration;
- a secret-backend pattern or env for secrets.

### 19.4 DAG tests

Every new DAG should have import/parse validation.

Where useful, test:

- task IDs;
- dependencies;
- schedule;
- retries;
- required params;
- absence of import errors.
