import logging
import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.trigger_rule import TriggerRule

from utils import notify_failure
from utils import bootstrap_schema

logger = logging.getLogger(__name__)

# Template combining the Airflow Variable and the execution logical date {{ ds }}
TEMPLATED_SETTLEMENT_PATH = (
    "{{ var.value.SETTLEMENT_FILE_DIR }}/settlement_{{ ds }}.csv"
)

@dag(
    dag_id="bank_settlement_sensor_pipeline",
    start_date=datetime(2026, 8, 4),
    schedule="0 6 * * *", # Starts daily at 6:00 AM UTC
    catchup=False,
    default_args={"on_failure_callback": notify_failure},
    tags=["finance", "treasury", "sensors"],
) 

def bank_settlement_sensor_pipeline():
    # Sensor Task: Wait for file arrival
    wait_for_settlement_file = FileSensor(
        task_id="wait_for_settlement_file",
        filepath=TEMPLATED_SETTLEMENT_PATH,
        poke_interval=300, # Check every 300 seconds,
        timeout=timedelta(hours=7), # 7 hours cutoff duration
        mode="reschedule", # releases worker slots back to pool while sleeping
        soft_fail=False, # Fail tasks loudly to trigger alerts
    )

    @task()
    def process_settlement_file(filepath: str) -> None:
        """Reads settlement CSV, validates row count, and persists an audit record into Postgres."""
        context = get_current_context()
        settlement_date_str = context["logical_date"].strftime("%Y-%m-%d")

        logger.info("Processing settlement file for date %s at: %s", settlement_date_str, filepath)

        if not os.path.exists(filepath):
            raise FileNotFoundError(
                f"Processing Failed: Sensor marked success, but File expected at {filepath} does not exist."
            )

        # Read file and count record (excluding header)
        with open(filepath, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]

        total_lines = len(lines)
        data_row_count = max(0, total_lines - 1)  # Subtract header row

        logger.info(
            "Settlement file read successfully. Total lines: %d (Data rows: %d)",
            total_lines,
            data_row_count,
        )

        # Persist execution record in processed_settlement_files
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        upsert_settlement_sql = """
            INSERT INTO processed_settlement_files (settlement_date, filepath, row_count, processed_at)
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (settlement_date)
            DO UPDATE SET
                filepath = EXCLUDED.filepath,
                row_count = EXCLUDED.row_count,
                processed_at = CURRENT_TIMESTAMP
        """

        # Guarantee entire production schema exists safely under advisory lock
        bootstrap_schema("my_postgres_conn")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(upsert_settlement_sql, 
                               (settlement_date_str, filepath, data_row_count),
                )
                conn.commit()

        logger.info(
            "✓ [AUDIT LOGGED] Recorded settlement date %s in processed_settlement_files (%d rows).",
            settlement_date_str,
            data_row_count,
        )

    # Cross-DAG Trigger: Kicks off reconciliation_dag upon successful settlement processing
    trigger_reconciliation = TriggerDagRunOperator(
        task_id="trigger_reconciliation_dag",
        trigger_dag_id="reconciliation_dag",
        logical_date="{{ logical_date }}",
        reset_dag_run=True, # Clears & re-runs existing DAG run if re-triggered for same date
        wait_for_completion=False, # Fire-and-forget; doesn't block worker waiting for completion
        trigger_rule=TriggerRule.ALL_DONE, # Runs whether process_settlement_file succeeded or upstream failed
    )

    # Setting dependency, passing TEMPLATED_SETTLEMENT_PATH to the @task function lets Airflow
    # render the Jinja string before executing the task body.
    # Dependency Chain: Sensor -> Process -> Trigger
    (
        wait_for_settlement_file
        >> process_settlement_file(TEMPLATED_SETTLEMENT_PATH)
        >> trigger_reconciliation
    )


dag_instance = bank_settlement_sensor_pipeline() 