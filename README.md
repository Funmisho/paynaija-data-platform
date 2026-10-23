# PayNaija Data Platform

Four Apache Airflow DAGs built, using a fictional Nigerian fintech, PayNaija, as the business backdrop. Each DAG started as a business problem, went through a design review before any code was written, and got debugged against a Postgres database. The whole platform now runs in Docker Compose, so one command brings up Airflow and both databases.

Full business requirements for all eight projects that built this system: [`docs/business-requirements.pdf`](docs/business-requirements.pdf). Full design reasoning and debugging notes: [`docs/design-decisions.md`](docs/design-decisions.md).

## Table of contents

- [What this is](#what-this-is)
- [Architecture](#architecture)
- [The four pipelines](#the-four-pipelines)
- [Tech stack](#tech-stack)
- [Repository structure](#repository-structure)
- [Quickstart (Docker)](#quickstart-docker)
- [Configuration](#configuration)
- [Stopping, resetting and troubleshooting](#stopping-resetting-and-troubleshooting)
- [Running against Azure Postgres](#running-against-azure-postgres)
- [Running without Docker](#running-without-docker)
- [Known limitations](#known-limitations)
- [Screenshots](#screenshots)

## What this is

PayNaija needed daily customer signups loaded into a reporting table, daily exchange rates for finance, a way to wait for a bank settlement file that arrives at an unpredictable time each morning, and a weekly reconciliation report tying all three together. Each of those became its own DAG, built one at a time, with the difficulty increasing project by project.

The point of this project was not to practice Airflow syntax. It was to think through the questions a data engineer actually faces: what happens on retry, what happens when a value is missing, what happens when two runs overlap, and what happens when something upstream fails outright.

## Architecture

![Architecture overview](docs/architecture-diagram.png)

**The pipelines.** `signups_etl_pipeline` and `exchange_rates_etl_pipeline` run independently on their own daily schedules. `bank_settlement_sensor_pipeline` waits for a file, then triggers `reconciliation_dag` directly. `reconciliation_dag` also has its own independent daily schedule as a backstop, so it never depends entirely on another DAG's health to run. The full reasoning behind that dual-trigger design is in [`docs/design-decisions.md`](docs/design-decisions.md).

**The containers.** Docker Compose runs six services:

| Service | What it does |
|---|---|
| `airflow-db` | Postgres holding Airflow's own metadata (DAG runs, task states). Not published to the host. |
| `paynaija-warehouse` | Postgres holding the business tables. Published to `127.0.0.1:5433` only, so DBeaver, pgAdmin or `psql` on your machine can reach it. |
| `airflow-init` | One-shot container. Builds the image, migrates the metadata database, creates the admin user, then exits. |
| `airflow-webserver` | The UI, on port 8080. |
| `airflow-scheduler` | Schedules and runs tasks (LocalExecutor, so tasks run inside this container). |
| `airflow-triggerer` | Runs deferrable operators. Included so they work later; none of the DAGs use one yet. |

The webserver, scheduler and triggerer share one image (`paynaija-airflow:2.9.1`) and differ only in the command they start with. Keeping the metadata database and the warehouse in separate containers means either can be wiped or upgraded without touching the other.

## The four pipelines

### signups_etl_pipeline

| | |
|---|---|
| Schedule | `@daily` |
| Source | `signups.csv` (path in an Airflow Variable) |
| Destination | `customer_signups` (Postgres) |
| Pattern | Run-scoped staging table, `ON CONFLICT (signup_id) DO NOTHING` |
| Key concerns | Leading-zero ID preservation, rejected-row auditability, per-task retry tuning |

Reads a daily signups CSV, separates valid rows from invalid ones, and loads the valid rows into Postgres. Tasks: `extract`, `clean_and_split_data`, `load_data`.

Rejected rows never disappear. Missing `signup_id`, missing `email`, and duplicate rows all land in `rejected_signups.csv` with a `rejection_reason` column. `signup_id` is read as a string at every point this pipeline touches a CSV, so a value like `00123` is never silently coerced into `123`. Retries are tuned per task: `extract` and `clean_and_split_data` fail fast, since their failures are deterministic, while `load_data` retries with exponential backoff, since its failures are usually a transient database issue.

Full reasoning: [design-decisions.md § signups_etl_pipeline](docs/design-decisions.md#signups_etl_pipeline).

### exchange_rates_etl_pipeline

| | |
|---|---|
| Schedule | `@daily` |
| Source | Public FX API (NGN, GHS, KES against USD) |
| Destination | `clean_exchange_rates` or `flagged_exchange_rates`, branch-dependent |
| Pattern | Bronze/silver (append-only raw audit log, upserted clean table), branching on an anomaly threshold |
| Key concerns | Race-safe raw lookup by row id, cold-start handling, missing-currency tolerance |

Pulls daily exchange rates and prepares them for finance reporting. Tasks: `extract_raw_exchange_rates`, `transform_exchange_rates`, `check_for_anomalies`, `route_decision`, then either `load_clean_exchange_rates` or `flag_anomalous_rates`.

`raw_exchange_rates` is append only, so any historical API response can be inspected later. A missing currency is a warning, not a hard failure, the pipeline loads what it has. Any currency whose rate swings more than `ANOMALY_THRESHOLD_PERCENTAGE` (an Airflow Variable, 15 percent by default) routes the whole run to `flagged_exchange_rates` instead of production, with a direct notification fired from inside that task.

Full reasoning: [design-decisions.md § exchange_rates_etl_pipeline](docs/design-decisions.md#exchange_rates_etl_pipeline).

### bank_settlement_sensor_pipeline

| | |
|---|---|
| Schedule | Daily, 6:00 AM |
| Source | Settlement file dropped by Treasury Ops, arrival time unpredictable |
| Destination | `processed_settlement_files` (audit), plus triggers `reconciliation_dag` |
| Pattern | `FileSensor` in `reschedule` mode, cross-DAG trigger |
| Key concerns | Worker-slot cost of long waits, exact-path Jinja templating, `logical_date` propagation |

Waits for a file that historically lands anywhere from 6am to past 11am, sometimes not at all. Tasks: `wait_for_settlement_file`, `process_settlement_file`, `trigger_reconciliation`.

The sensor polls every five minutes without holding a worker slot the whole time, gives up after seven hours, and fails loudly rather than silently skipping. `trigger_reconciliation` fires on `TriggerRule.ALL_DONE`, so a missing file still reaches reconciliation the same day, and it explicitly passes `logical_date` forward, since Airflow does not do that automatically.

Full reasoning: [design-decisions.md § bank_settlement_sensor_pipeline](docs/design-decisions.md#bank_settlement_sensor_pipeline).

### reconciliation_dag

| | |
|---|---|
| Schedule | Triggered (`ALL_DONE`) plus an independent daily 2:00 PM backstop |
| Source | `customer_signups`, `clean_exchange_rates`, `flagged_exchange_rates`, `processed_settlement_files` |
| Destination | `weekly_reconciliation_reports` |
| Pattern | Date-backbone aggregation query, idempotent weekly upsert, dual-trigger resilience |
| Key concerns | Honest gap reporting mid-week, tables guaranteed to exist on a cold start, resilience to total upstream failure |

Builds the weekly reconciliation report. Single task: `build_weekly_reconciliation`.

The report status is not binary, it can be `IN_PROGRESS`, `IN_PROGRESS_WITH_GAPS`, `IN_PROGRESS_WITH_ANOMALIES`, `PENDING_AUDIT_GAPS`, `PENDING_FX_REVIEW`, or `FINAL`, and gaps or anomalies surface as soon as they are detected rather than waiting for the week to close. Because this DAG queries four tables it does not own, it calls the shared `bootstrap_schema()` helper before querying, the same helper every other DAG calls, so it never crashes on a fresh database or on a branch that has never run.

Full reasoning: [design-decisions.md § reconciliation_dag](docs/design-decisions.md#reconciliation_dag).

## Tech stack

Apache Airflow 2.9.1 (TaskFlow API, LocalExecutor), Python 3.10 (pandas below 2.2, numpy, requests, psycopg2), PostgreSQL 16, Docker and Docker Compose. In production use the warehouse is Azure Database for PostgreSQL (Flexible Server); any Postgres 13+ works for local development.

## Repository structure

```
paynaija-data-platform/
├── dags/
│   ├── signups_etl_pipeline.py
│   ├── exchange_rate_pipeline.py
│   ├── bank_settlement_sensor_pipeline.py
│   ├── reconciliation_dag.py
│   ├── utils.py            # notify_failure callback + bootstrap_schema() (the warehouse DDL)
│   └── .airflowignore      # keeps the scheduler from parsing utils.py as a DAG
├── docs/
│   ├── architecture-diagram.png
│   ├── design-decisions.md
│   ├── business-requirements.pdf
│   └── screenshots/
├── data/
│   ├── signups.csv
│   └── settlement_sample.csv
├── Dockerfile              # Airflow 2.9.1 image with the Python requirements baked in
├── docker-compose.yml      # the six services above
├── .env.example            # every variable the stack needs, with placeholders
├── requirements.txt
├── .gitignore
└── README.md
```

`utils.py` sits in `dags/` because it needs to be importable from every DAG file with a plain `from utils import ...`, but it has no `@dag` decorator, so it produces nothing for Airflow's scheduler to run. It is listed in `dags/.airflowignore` so the scheduler does not waste time parsing it on every cycle.

All warehouse table definitions live in `utils.py`. `bootstrap_schema()` takes a Postgres advisory lock before running `CREATE TABLE IF NOT EXISTS`, so DAGs that start at the same moment on an empty database queue up instead of racing each other.

## Quickstart (Docker)

You need Docker with Compose v2 and Git. Everything below runs from the repository root.

1. **Clone and create your env file.**

   ```bash
   git clone https://github.com/Funmisho/paynaija-data-platform.git
   cd paynaija-data-platform
   cp .env.example .env
   mkdir -p logs
   ```

   `.env` holds every secret and is git-ignored. `logs/` is git-ignored too, so it has to be created before the first start; if Docker creates it for you it ends up owned by root and Airflow cannot write to it.

2. **Build the image** (the next step uses it).

   ```bash
   docker compose build airflow-init
   ```

3. **Generate the two Airflow keys** using the image you just built, so nothing needs installing on your machine:

   ```bash
   docker run --rm --entrypoint python paynaija-airflow:2.9.1 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   docker run --rm --entrypoint python paynaija-airflow:2.9.1 -c "import secrets; print(secrets.token_hex(32))"
   ```

4. **Fill in `.env`.** Paste the first value as `AIRFLOW__CORE__FERNET_KEY` and the second as `AIRFLOW__WEBSERVER__SECRET_KEY`. Set `AIRFLOW_UID` to the output of `id -u`. Set passwords for both databases and for the Airflow admin login. Passwords end up inside connection URLs, so use letters and digits only, for example `openssl rand -hex 16`. Compose refuses to start if a required secret is missing.

5. **Start everything.**

   ```bash
   docker compose up -d
   ```

6. **Check that it worked.**

   ```bash
   docker compose ps -a
   docker inspect -f '{{.State.ExitCode}}' paynaija-airflow-init    # should print 0
   docker compose exec airflow-scheduler airflow dags list-import-errors   # should print "No data found"
   ```

   `airflow-init` should show as `Exited (0)` and the other five as running or healthy.

7. **Open the UI** at <http://localhost:8080> and log in with `AIRFLOW_WWW_USER` (default `admin`) and `AIRFLOW_WWW_PASSWORD` from your `.env`. All DAGs start paused.

8. **Run the pipelines in this order.** Unpause each DAG, then trigger it:

   1. `signups_etl_pipeline`. Its input is `data/signups.csv`, already in the repo.
   2. `exchange_rates_etl_pipeline`. It calls the public FX API, so it needs internet access from the container.
   3. `bank_settlement_sensor_pipeline`. First copy the sample file to the exact name the sensor is waiting for: `data/settlement_sample.csv` becomes `settlement_<YYYY-MM-DD>.csv` for the run's date, inside the folder `SETTLEMENT_FILE_DIR` points to. The sensor matches an exact path, so a close name counts as no file. When it succeeds it triggers `reconciliation_dag`.

9. **Look at the result.**

   ```bash
   docker compose exec paynaija-warehouse sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "\dt"'
   ```

   Six tables should exist. You do not need to create any of them: the first DAG to run builds the whole schema.

## Configuration

Everything is configured through `.env`. Nothing is entered by hand in the Airflow UI, so a fresh clone needs no manual setup after `docker compose up`.

**Secrets and credentials.** Database users, passwords, the admin login, `AIRFLOW_UID`, the Fernet key and the webserver secret key. See `.env.example` for the full list.

**The warehouse connection.** `docker-compose.yml` builds the `my_postgres_conn` connection from the warehouse variables and passes it to every Airflow container as an `AIRFLOW_CONN_MY_POSTGRES_CONN` environment variable. A `fs_default` connection for the settlement `FileSensor` is defined the same way. DAG code only ever refers to the connection id.

**Airflow Variables.** Each Variable is an `AIRFLOW_VAR_<NAME>` entry in `.env`:

| Variable | Purpose |
|---|---|
| `SIGNUPS_INPUT_PATH` | Input CSV for the signups pipeline, for example `/opt/airflow/data/signups.csv` |
| `SIGNUPS_CLEAN_PATH` | Where cleaned signups are written |
| `SIGNUPS_REJECTED_PATH` | Where rejected signups are written |
| `SETTLEMENT_FILE_DIR` | Folder the settlement sensor watches |
| `EXCHANGE_RATE_API_URL` | FX API endpoint |
| `REQUIRED_CURRENCIES` | JSON list, for example `["NGN", "GHS", "KES"]` |
| `ANOMALY_THRESHOLD_PERCENTAGE` | Day-over-day swing that flags a run, 15 by default |

File paths are paths **inside the container**. The repository's `data/` folder is mounted at `/opt/airflow/data`, so a path like `/home/you/...` will not exist there.

Connections and Variables defined through the environment do not appear on the Admin pages of the UI. Check them from the command line instead:

```bash
docker compose exec airflow-scheduler airflow connections get my_postgres_conn
docker compose exec airflow-scheduler airflow variables get SIGNUPS_INPUT_PATH
```

**Timezones.** Schedules run in UTC. The UI displays times in `Africa/Lagos`.

**The Fernet key.** Airflow encrypts passwords stored in its metadata database with `AIRFLOW__CORE__FERNET_KEY`. Replacing the key makes anything saved through the UI unreadable. Connections and Variables from the environment are not stored encrypted, so this stack is not affected, but keep the key stable anyway. To change it safely, put the new key first and the old one after it, separated by a comma, run `airflow rotate-fernet-key`, then drop the old one.

## Stopping, resetting and troubleshooting

```bash
docker compose down          # stop and remove containers, keep all data
docker compose down -v       # also delete both Postgres volumes: metadata AND warehouse
docker compose up -d --build # rebuild the image after changing requirements.txt
```

`down -v` is the full reset and cannot be undone. DAG changes need no rebuild, since `dags/` is mounted live.

**The UI will not open, or tasks fail with a permission error on `logs/`.** The bind-mounted folders must be writable by the user the containers run as. Set `AIRFLOW_UID` in `.env` to the output of `id -u`, then:

```bash
mkdir -p logs dags data
sudo chown -R "$(id -u):0" logs dags data
docker compose up -d
```

**`airflow-init` fails.** Read its log first: `docker compose logs airflow-init`. A missing variable in `.env` is the most common cause, and Compose names the variable in the error.

**A task fails with "Executor reports task instance finished (failed) although the task says it's queued".** The task process was killed before it could log anything. On Windows with WSL, check that Docker has enough memory. Five containers plus a pandas task need a few GB.

**Port already in use.** Change the host side of the `ports:` entry in `docker-compose.yml`. The warehouse is published on `5433` so it does not clash with a Postgres already running on `5432`.

## Running against Azure Postgres

The DAG code does not change between local and Azure. Only the warehouse connection details change, and they come from environment variables, not from code.

1. Provision an Azure Database for PostgreSQL Flexible Server. A burstable B2s tier is sufficient for this workload.
2. Add a firewall rule allowing your client IP, and keep SSL enforced.
3. Create a second env file, `.env.azure`, next to `.env`. It is git-ignored. Copy `.env` and change the warehouse values:

   ```text
   WAREHOUSE_HOST=<your-server-name>.postgres.database.azure.com
   WAREHOUSE_PORT=5432
   WAREHOUSE_DB=<your-database-name>
   WAREHOUSE_USER=<your-admin-username>
   WAREHOUSE_PASSWORD=<your-admin-password>
   WAREHOUSE_SSLMODE=require
   ```

4. Start the stack with that file:

   ```bash
   docker compose --env-file .env.azure up -d
   ```

Airflow's own metadata database stays in the local `airflow-db` container either way; only the business warehouse moves. The local `paynaija-warehouse` container still starts, but nothing reads from it.

The first run creates any missing tables through `bootstrap_schema()`. `CREATE TABLE IF NOT EXISTS` never alters a table that already exists, so if the Azure database was created by an earlier version of this project, compare its tables with the definitions in `dags/utils.py` before relying on it.

## Running without Docker

Docker is the supported path, but the DAGs run on a plain Airflow install too. Use the same version the image uses:

```bash
python3.10 -m venv .venv && source .venv/bin/activate
pip install "apache-airflow[postgres]==2.9.1" -r requirements.txt \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.1/constraints-3.10.txt"
export AIRFLOW_HOME="$PWD"
export AIRFLOW_CONN_MY_POSTGRES_CONN="postgresql://<user>:<password>@localhost:5432/<database>"
airflow standalone
```

Then set the Variables from the [Configuration](#configuration) table as `AIRFLOW_VAR_<NAME>` environment variables (or in the UI) with paths that exist on your machine, and run the pipelines in the order given in step 8 of the quickstart.

## Known limitations

- **LocalExecutor.** Tasks run inside the scheduler container. That is fine for four DAGs, and the move to CeleryExecutor or KubernetesExecutor would be the first change for real load.
- **No schema migrations.** `CREATE TABLE IF NOT EXISTS` builds a missing table but never changes an existing one, so adding a column later needs a migration tool such as Alembic.
- **Notifications are simulated.** `notify_failure` and the anomaly alert write the email they would send to the task log instead of sending it.
- **Local credentials are env-file based.** Fine for development. A deployed version would read them from a secrets manager such as Azure Key Vault.

## Screenshots

### Exchange rate anomaly branch

![Exchange rate anomaly branch](docs/screenshots/exchange-rates-anomaly.png)

Shows `check_for_anomalies` routing the run to `flag_anomalous_rates` while `load_clean_exchange_rates` is skipped.

### Normal exchange rate run

![Normal exchange rate run](docs/screenshots/exchange-rates-normal.png)

Shows the normal branch where the exchange rates pass validation and are loaded into `clean_exchange_rates`.

### Signups ETL pipeline

![Signups ETL pipeline](docs/screenshots/signups-etl.png)

Successful end-to-end run of `signups_etl_pipeline`, with extraction, cleaning, and loading tasks completing successfully.

### Settlement file sensor

![Settlement file sensor](docs/screenshots/settlement-sensor.png)

Shows the `FileSensor` polling for the settlement file, waiting through the first two checks, then detecting the file on the third check and allowing the pipeline to continue.