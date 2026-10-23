# Design decisions and debugging notes

This document holds the longer reasoning behind the PayNaija Data Platform: why certain design choices were made, what tradeoffs were considered and rejected, and what actually broke the first time each pipeline ran against a real database, and later inside Docker. The main [README](../README.md) covers what the system does and how to run it. This covers why it works the way it does.

## Contents

- [Cross-cutting design decisions](#cross-cutting-design-decisions)
- [Per-pipeline reasoning](#per-pipeline-reasoning)
- [Docker setup](#docker-setup)
- [What testing found](#what-testing-found)
- [Known limitations](#known-limitations)

## Cross-cutting design decisions

**Idempotency is the actual throughline.** Every load in this repo, signups, clean exchange rates, flagged exchange rates, settlement audit records, and weekly reports, is built to be safely re-run. It is the same question asked in a new context each time: what is the natural key, and what happens if this exact task runs twice. `signups_etl_pipeline` answers it with a run-scoped staging table and `ON CONFLICT (signup_id) DO NOTHING`. `exchange_rates_etl_pipeline` answers it differently for two tables in the same DAG: `raw_exchange_rates` is append-only, since every fetch is a distinct historical event worth keeping, while `clean_exchange_rates` upserts on `(logical_date, currency_code)`, since it represents current known state and a same-day correction should overwrite. `reconciliation_dag` upserts on `week_start_date`, so recomputing mid-week or twice within minutes is a no-op, not a duplicate.

**Config has a real boundary, not a reflex.** File paths, the FX API URL, the required currency list, and the anomaly threshold live in Airflow Variables, editable without a code deploy. Retry counts, table names, and sensor timing values stayed in code, since those are implementation details a data engineer would change alongside other code, not something operations needs to tweak independently. `Variable.get()` is never called at DAG file top level, only inside task bodies, since a top-level call re-executes on every scheduler parse cycle, not just when the DAG runs, which is a documented cause of scheduler slowdown in production Airflow.

**Two kinds of alert, two mechanisms.** An anomaly is a business condition the pipeline handled correctly, so it gets a direct notification from inside the task where it happened (`flag_anomalous_rates`). A genuine Airflow failure, an uncaught exception or a sensor timeout, gets a shared `on_failure_callback` (`notify_failure`, in `utils.py`) set once at the DAG level, so a task added later is covered automatically rather than depending on someone remembering to wire it in per task.

**Every DAG assumes its neighbors can fail.** A missing currency does not kill a whole day's FX load, it logs what is missing and proceeds with what is valid. A missing settlement file does not silently corrupt the reconciliation report, it gets reported as missing, the same day, via a hybrid trigger described below. None of this was designed in one pass. Most of it came from tracing a specific failure scenario end to end until a real gap showed up.

**The schema has one owner.** Every warehouse table is defined once, in `utils.py`, and every DAG calls `bootstrap_schema()` inside a task before it touches the database. This replaced an earlier arrangement where each DAG created the tables it needed and the reconciliation DAG created all of them. Having two copies of the same DDL meant `IF NOT EXISTS` would hide any difference between them, and whichever DAG happened to run first on an empty database would decide the real schema. The reason it lives inside tasks and not at module level is the same rule as for `Variable.get()`: no database work at DAG parse time.

## Per-pipeline reasoning

### signups_etl_pipeline

The load is idempotent. Every run builds a run-scoped staging table cloned from `customer_signups` using `LIKE ... INCLUDING ALL`, so a type mismatch gets caught by Postgres instead of silently coerced by pandas, and merges into production with:

```sql
INSERT INTO customer_signups (
    signup_id, full_name, email, phone_number,
    signup_date, referral_source, plan_type
)
SELECT signup_id, full_name, email, phone_number,
       signup_date, referral_source, plan_type
FROM staging_customer_signups_<run_id>
ON CONFLICT (signup_id) DO NOTHING;
```

A `try/finally` block guarantees the staging table gets dropped even if the merge fails partway through, so a bad retry does not leave orphaned tables behind. The staging table is the one piece of DDL that stays inside this DAG: it exists per run, so it is not part of the shared schema and cannot race with anything.

Rejected rows never disappear. Missing `signup_id`, missing `email`, and duplicate rows all land in `rejected_signups.csv` with a `rejection_reason` column, so if finance asks why yesterday's signup count looks low, there is an actual answer.

`signup_id` is read as a string at every point this pipeline touches a CSV. An earlier draft let pandas infer the type and it silently turned `00123` into `123`.

Retries are tuned per task rather than applied as a blanket setting. `extract` and `clean_and_split_data` do not retry, because their failures are deterministic and retrying just delays the same outcome. `load_data` retries with exponential backoff, because its failures are usually a transient database issue.

### exchange_rates_etl_pipeline

The raw API response and the cleaned version live in two separate tables. `raw_exchange_rates` is append only, every fetch gets its own row, nothing is ever updated or deleted, so a question like "what did the API actually return on a given day" always has an answer. `transform_exchange_rates` reads that raw JSON back by its exact row id rather than by date, since two runs on the same day would otherwise race each other.

A missing currency is treated as a warning, not a hard failure. If GHS is temporarily absent from the API response but NGN and KES came through, the pipeline still loads what it has and logs exactly what is missing, rather than losing all three currencies over one gap.

Branching handles anomalies. After a real incident where a bad GHS rate, a 40 percent overnight jump from a bad data point at the provider, went straight into the reporting table unnoticed, `check_for_anomalies` now compares each new rate against yesterday's loaded rate before anything is written. Any currency that swings more than `ANOMALY_THRESHOLD_PERCENTAGE` (an Airflow Variable, 15 percent by default) routes the whole run to `flagged_exchange_rates` instead of production, along with the previous rate and the percentage change, so a reviewer sees the full picture without a second query.

The branch computation and the branch decision are split into two tasks on purpose. `check_for_anomalies` does the math and packages a decision. `route_decision`, the task actually decorated with `@task.branch()`, just reads that decision and tells Airflow which path to take. This made each part easier to reason about on its own, and it means the Airflow Grid view shows exactly which path a given day took without digging into logs.

The first run of this DAG has no "yesterday" to compare against. Treating that cold start as anomalous by default would mean day two also has no valid baseline, since nothing loaded on day one, which creates a permanent lockout. Cold start is treated as normal instead.

An anomaly triggers a direct alert from inside `flag_anomalous_rates`, separate from the operational failure callback. An anomaly is not Airflow failing at anything, it is the branch working exactly as designed, so it needed its own notification path rather than being folded into the generic failure alert.

### bank_settlement_sensor_pipeline

The sensor polls every five minutes in `mode="reschedule"`, which releases its worker slot between checks instead of holding it for potentially hours. It gives up after seven hours and fails loudly rather than silently skipping, since a missing file needs to page someone, not vanish quietly.

The settlement file path is Jinja templated: `{{ var.value.SETTLEMENT_FILE_DIR }}/settlement_{{ ds }}.csv`, so each run resolves to that day's actual file rather than assuming there is only ever one file.

Once `process_settlement_file` reads the file, it writes a row to `processed_settlement_files`, a durable audit table recording the settlement date, the filepath, and the row count. This table did not exist in the original design. It only became necessary once `reconciliation_dag` needed a real way to know which days actually succeeded.

`trigger_reconciliation` fires `reconciliation_dag` with `trigger_rule=TriggerRule.ALL_DONE` rather than the default `all_success`, so it runs whether settlement processing succeeded or failed. The first version used the default trigger rule, which meant a missing settlement file silently prevented reconciliation from running at all that day, with no signal to finance until some later successful run happened to catch the gap retroactively.

The trigger also explicitly passes `logical_date="{{ logical_date }}"`. Airflow does not automatically propagate a triggering DAG's logical date to the DAG it triggers. Left unset, the triggered run's logical date defaults to whenever the trigger physically fires, which would have quietly broken the weekly window calculation on any manual re-run or backfill.

### reconciliation_dag

One consolidated SQL query builds a date backbone for the Monday through Sunday week containing the run's logical date, using `generate_series` up to the earlier of the week's end and the logical date, then left joins it against `customer_signups`, `clean_exchange_rates`, `flagged_exchange_rates`, and `processed_settlement_files`. Mid-week, the backbone stops at the logical date, so a Thursday run audits four days, not seven.

The report status is not binary. A week can be `IN_PROGRESS`, `IN_PROGRESS_WITH_GAPS`, `IN_PROGRESS_WITH_ANOMALIES`, `PENDING_AUDIT_GAPS`, `PENDING_FX_REVIEW`, or `FINAL`. Gaps and anomalies surface in the status as soon as they are detected, mid-week, rather than being held back until Sunday. An earlier draft checked whether the week was over before checking whether anything was wrong, which meant a Tuesday settlement gap looked identical to a fully healthy week until Sunday finally arrived. Reordering those checks changed what the report actually communicated, not just how it was written.

Since this DAG queries four tables it does not own, it has to be able to run against a database where none of the upstream DAGs have ever run, or where a branch like `flag_anomalous_rates` has genuinely never been taken. It originally solved this by creating all of those tables itself. It now calls the same `bootstrap_schema()` helper as every other DAG, so it remains a safety net for a cold start without being the only owner of the schema. The helper creates tables in an order that respects the foreign key from `clean_exchange_rates` and `flagged_exchange_rates` back to `raw_exchange_rates`.

This DAG has two ways of running, on purpose. A fast path fires the moment `bank_settlement_sensor_pipeline` finishes, success or failure. An independent daily schedule at 14:00 acts as a backstop that does not depend on any other DAG's health at all, so even if `bank_settlement_sensor_pipeline` itself were paused or broken, reconciliation would still run and could report zero settlements processed that day, rather than staying silently frozen on stale data. The two can fire within minutes of each other on a normal day. Since everything upserts on `week_start_date`, running the same aggregation twice just recomputes the same result rather than duplicating anything.

## Docker setup

The pipelines were first developed against a hand-installed Airflow. Containerizing them was meant to make the whole platform reproducible: one command, no manual steps in the UI, and a result that behaves the same on a stranger's machine. These are the decisions that shaped it.

**Separate containers, one image.** The webserver, scheduler and triggerer each get their own container, because Docker only supervises the container's main process. If everything ran in one container, one process crashing would take the others down with it, and the container's health would say nothing about which part failed. All three use the same custom image and differ only in the command they start with, so there is one place where dependencies are installed. Three near-identical Dockerfiles would share layers on disk, so disk space was not the real argument. The real one is drift: someone bumps a dependency in one Dockerfile and forgets the others, and the scheduler ends up running different code than the webserver.

**Dependencies baked in, DAGs mounted.** `requirements.txt` is copied and installed in its own layer before anything else, so it only rebuilds when the requirements change. `dags/`, `logs/` and `data/` are bind mounts, so editing a DAG needs no rebuild. The `pandas` version is capped below 2.2 in `requirements.txt`, for the same reason as the `to_sql` incompatibility listed under testing below: Airflow 2.9 ships with SQLAlchemy 1.4.

**Two Postgres containers.** Airflow's metadata (DAG runs, task states, connections stored through the UI) and the business warehouse are different systems with different lifecycles, so they live in separate containers with separate volumes and separate credentials. The metadata database can be dropped, reset or upgraded without any risk to business data, and a bad `docker volume rm` on one cannot reach the other. This is not total isolation: both containers still share the host machine, the Docker daemon and one `.env` file, so it protects against Airflow-level problems, not against losing the disk. The metadata container publishes no port at all, since only other containers need it. The warehouse is published on `127.0.0.1:5433` only, so tools on the host can reach it but the rest of the network cannot, and 5433 keeps it clear of a Postgres already running on the host's 5432.

**One init container.** `airflow-init` builds the image, migrates the metadata database, creates the admin user, and exits. The other Airflow services wait for it with `condition: service_completed_successfully`, so the scheduler can never start against an unmigrated database. It uses the official image's environment-variable mechanism (`_AIRFLOW_DB_MIGRATE`, `_AIRFLOW_WWW_USER_CREATE`) with a trivial command, which keeps the actual work inside the image's own entrypoint. The tradeoff is that the work is less visible than an explicit script would be. It runs again on every `docker compose up`, so it has to be safe to repeat: migrating an already-migrated database is a no-op, and the entrypoint skips creating a user that already exists.

**Secrets live in `.env`, not in the repository.** Compose reads `.env` and substitutes the values into `docker-compose.yml`. Every secret uses the `${VAR:?message}` form, so a missing value stops the stack with a message naming the variable, instead of silently falling back to a weak default. Non-secrets such as usernames and database names do have defaults. A committed `.env.example` lists every variable with placeholders, and passwords are restricted to URL-safe characters because they end up inside connection URLs.

**Connections and Variables come from the environment.** The warehouse connection is an `AIRFLOW_CONN_MY_POSTGRES_CONN` variable, the `FileSensor`'s `fs_default` connection is defined the same way, and each Airflow Variable is an `AIRFLOW_VAR_<NAME>` variable. That makes a fresh `docker compose up` fully configured with no clicking in the UI, and it means rebuilding the metadata database does not lose any configuration. Environment-defined values do not show on the UI's Connections and Variables pages, which is expected. File paths in the Variables are container paths under `/opt/airflow/data`, not paths on the host.

**Fernet and webserver secret keys.** Airflow encrypts passwords stored in its metadata database with the Fernet key, and the webserver and scheduler use the secret key to sign requests to each other, for example when fetching task logs. Both are set explicitly and shared by every Airflow container. If each container generated its own, UI-stored secrets could not be decrypted across containers and log fetching would fail with 403 errors. Replacing a Fernet key outright makes anything already stored under the old one unreadable. Airflow accepts a comma-separated list with the new key first, and `airflow rotate-fernet-key` re-encrypts everything, which is the safe way to change one. This stack is partly shielded, since its connections and variables never go into the database, but the key still needs to stay stable for anything entered through the UI.

**Schema bootstrap under an advisory lock.** The warehouse is a new empty database on every fresh clone, so the DAGs have to build their own tables. `CREATE TABLE IF NOT EXISTS` looks safe to run concurrently, but Postgres does not make the existence check and the creation atomic, so two DAGs starting at the same moment on an empty database can still collide. `bootstrap_schema()` therefore takes `pg_advisory_xact_lock` inside the same transaction as the DDL. The second caller waits, then finds the tables already there. The lock is released automatically when the transaction commits.

An init SQL script mounted into the warehouse container's `docker-entrypoint-initdb.d` would also have worked, but it only runs on the first start of an empty volume and would not help when the same DAGs point at an Azure database. Keeping the schema in the DAGs covers both. The cost is stated under limitations below.

**Local or Azure, same file.** The warehouse host, port and SSL mode are variables in the connection string, so pointing the stack at Azure Database for PostgreSQL is a second env file (`docker compose --env-file .env.azure up -d`) and no change to `docker-compose.yml` or any DAG. Hardcoding the Azure hostname into the compose file would have made switching a code change and made it easy to commit a real hostname by accident.

**The triggerer runs although nothing needs it yet.** None of the DAGs use deferrable operators, and the `FileSensor` runs in `reschedule` mode. The triggerer is included so a deferrable operator works the day one is added, and it costs one extra container.

## What testing found

Design review catches a lot. Every item below only surfaced by actually running these DAGs against a real Postgres instance. None of them were visible from reading the code.

### Against a hand-installed Airflow

- **A length mismatch in `signups_etl_pipeline`.** A rejection-reason column was computed against the full dataset with `np.select`, then assigned into a DataFrame already sliced down to a subset of it. Fixed by computing the reason column on the full DataFrame first, then slicing.
- **Three separate "table does not exist" failures** (`customer_signups`, `clean_exchange_rates`, `flagged_exchange_rates`), each a table that only got created inside a task that does not always run. The first fix was to make every task that depends on a table responsible for creating it. That fix itself had a second-order problem, covered in the next item.
- **A race between concurrent `CREATE TABLE IF NOT EXISTS` calls.** With several DAGs each creating tables, two of them starting at once on an empty database could fail, because the existence check and the creation are not atomic. Fixed by moving all DDL into one `bootstrap_schema()` helper that serializes callers with a Postgres advisory lock.
- **A pandas and SQLAlchemy version incompatibility**, not a bug in this codebase at all. `to_sql(method="multi")` breaks on certain pandas 2.2 plus, SQLAlchemy 1.4 pairings with a confusing error about an Engine or Connection object having no `cursor` attribute. Fixed by switching that insert to `psycopg2.extras.execute_values`, the same pattern already used elsewhere in this repo.
- **A Jinja path mismatch.** A test settlement file's name did not exactly match the rendered `{{ ds }}` path. A `FileSensor` matches on an exact literal path, so a close filename is treated the same as no file at all.
- **A foreign key ordering issue** in the table bootstrap, where `raw_exchange_rates` needed to be created before `clean_exchange_rates` and `flagged_exchange_rates`, since both reference it.

### Inside Docker

- **`uid not found: 1000` in `airflow-init`.** The container runs as the host user's UID so files on the bind mounts stay writable, but the image only knows the `airflow` user. The official image's entrypoint compensates by adding a passwd entry for the current UID. An early version of the init service overrode the entrypoint with `/bin/bash`, which skipped that step, and Airflow refused to start with an error that it was running as a user with no username. Fixed by keeping the image's entrypoint and passing the work as the command.
- **Bind-mount ownership on a fresh clone.** `logs/` is git-ignored, so it does not exist after cloning, and Docker creates a missing bind-mount folder owned by root. On a fresh clone the webserver would not open until `logs/`, `dags/` and `data/` were owned by the user the containers run as. The README now tells people to create `logs/` first and set `AIRFLOW_UID`, with a `chown` as the fallback.
- **A new metadata database has no Connections or Variables.** Moving to a container meant starting from an empty metadata database, which lost everything that had been entered by hand in the UI, and the `FileSensor`'s default `fs_default` connection was not present either. Both are now defined through the environment, which is why they survive a reset.
- **A task killed without a traceback.** One `signups_etl_pipeline` run failed in `clean_and_split_data` with only the scheduler's "executor reports the task finished although it says it's queued" message and nothing in the task log, which points at the task process being killed, not at a Python error. A laptop restart cleared it and it did not return. The cause was never confirmed. Memory pressure on Docker under WSL is the likeliest explanation, and it is why the README mentions memory.

The fresh-clone test is the check that ties these together: clone into an empty folder, fill in `.env`, run `docker compose up -d`, and confirm that all six warehouse tables exist after the first DAG run and that the init container exited with code 0.

## Known limitations

- **No schema migrations.** `CREATE TABLE IF NOT EXISTS` builds a missing table but never alters an existing one, so adding a column later would not reach a warehouse that already has the table. A migration tool such as Alembic is the next step.
- **LocalExecutor.** Tasks run inside the scheduler container, so a task that uses a lot of memory competes with the scheduler. Fine at this scale, not a design for heavy load.
- **Notifications are simulated.** Both alert paths write the email they would send to the task log instead of sending it.
- **Secrets in an env file.** Appropriate for local development. A deployed version would read them from a secrets manager.
- **Two containers still share a host.** The metadata and warehouse databases are isolated from each other's Airflow-level problems, not from the machine they run on.