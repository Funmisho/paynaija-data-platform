import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

def notify_failure(context: dict) -> None:
    """Shared Airflow failure callback for operational alerts."""
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    exception = context.get("exception", "No exception stack trace available.")
    logical_date_str = context["logical_date"].strftime("%Y-%m-%d %H:%M:%S")
    log_url = context["task_instance"].log_url

    logger.error(
        "\n===================================================================\n"
        "[EMAIL WOULD SEND] To: oncall@paynaija.com |\n"
        "Subject: Airflow Task Failed: %s.%s |\n"
        "Body: Task '%s' in DAG '%s' failed on logical_date %s.\n"
        "Exception: %s\n"
        "Log URL: %s\n"
        "===================================================================",
        dag_id,
        task_id,
        task_id,
        dag_id,
        logical_date_str,
        exception,
        log_url,
    )

# Complete System Schema Bootstrap (All dependencies + Report table)
SYSTEM_SCHEMA_BOOTSTRAP_SQL = """
    -- 1. Signups Production Table
    CREATE TABLE IF NOT EXISTS customer_signups (
        signup_id VARCHAR(50) PRIMARY KEY,
        full_name VARCHAR(255),
        email VARCHAR(255),
        phone_number VARCHAR(50),
        signup_date DATE,
        referral_source VARCHAR(100),
        plan_type VARCHAR(50)
    );

    -- 2. Raw Exchange Rates Table
    CREATE TABLE IF NOT EXISTS raw_exchange_rates (
        id SERIAL PRIMARY KEY,
        logical_date DATE NOT NULL,
        fetched_at TIMESTAMP NOT NULL,
        source_url VARCHAR(255) NOT NULL,
        raw_response JSONB NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_raw_rates_logical_date
    ON raw_exchange_rates (logical_date);

    -- 3. Clean Exchange Rates Table
     CREATE TABLE IF NOT EXISTS clean_exchange_rates (
        id SERIAL PRIMARY KEY,
        logical_date DATE NOT NULL,
        currency_code VARCHAR(3) NOT NULL,
        rate_to_usd NUMERIC(18, 6) NOT NULL,
        source_raw_id INT NOT NULL REFERENCES raw_exchange_rates(id),
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_date_currency UNIQUE(logical_date, currency_code)   
    );

    CREATE INDEX IF NOT EXISTS idx_clean_rates_date
    ON clean_exchange_rates (logical_date);

    -- 4. Flagged Exchange Rates Table (Crucial: prevents crash when anomaly branch hasn't run)
    CREATE TABLE IF NOT EXISTS flagged_exchange_rates (
        id SERIAL PRIMARY KEY,
        logical_date DATE NOT NULL,
        currency_code VARCHAR(3) NOT NULL,
        rate_to_usd NUMERIC(18, 6) NOT NULL,
        previous_rate_to_usd NUMERIC(18, 6),
        percentage_change NUMERIC(8, 4),
        source_raw_id INT NOT NULL REFERENCES raw_exchange_rates(id),
        flagged_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        CONSTRAINT unique_flagged_date_currency UNIQUE(logical_date, currency_code)
    );

    CREATE INDEX IF NOT EXISTS idx_flagged_rates_date
    ON flagged_exchange_rates (logical_date);

    -- 5. Bank Settlement Tracking Table
    CREATE TABLE IF NOT EXISTS processed_settlement_files (
        id SERIAL PRIMARY KEY,
        settlement_date DATE NOT NULL UNIQUE,
        filepath VARCHAR(255) NOT NULL,
        row_count INT NOT NULL,
        processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_settlement_date
    ON processed_settlement_files (settlement_date);

    -- 6. Weekly Reconciliation Reports Table
    CREATE TABLE IF NOT EXISTS weekly_reconciliation_reports (
        week_start_date DATE PRIMARY KEY,
        week_end_date DATE NOT NULL,
        last_evaluated_date DATE NOT NULL,
        generated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        status VARCHAR(30) NOT NULL,
        total_signups INT NOT NULL,
        days_settlement_processed INT NOT NULL,
        days_settlement_missing INT NOT NULL,
        anomalous_fx_count INT NOT NULL,
        reconciliation_payload JSONB NOT NULL
    );
""" 

def bootstrap_schema(postgres_conn_id: str = "my_postgres_conn") -> None:
    """
    Safely provisions all warehouse tables using Postgres transaction-level advisory locks.
    Prevents race conditions (duplicate key / tuple concurrent insert errors) when multiple
    DAGs attempt 'CREATE TABLE IF NOT EXISTS' simultaneously on a fresh database.
    """
    pg_hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            # Advisory lock (727001 = arbitrary fixed namespace key for PayNaija DDL)
            # Released automatically when tra nsaction terminates.
            cur.execute("SELECT pg_advisory_xact_lock(727001);")
            cur.execute(SYSTEM_SCHEMA_BOOTSTRAP_SQL)
        conn.commit()
    logging.info("Schema bootstrap verified successfully under advisory lock.")
