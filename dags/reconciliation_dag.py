import logging
import json
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json 

from utils import notify_failure
from utils import bootstrap_schema 

logger = logging.getLogger(__name__)


@dag(
    dag_id="reconciliation_dag",
    start_date=datetime(2026, 8, 1),
    schedule="0 14 * * *",  # Daily 2:00 PM guaranteed backstop schedule
    catchup=False,
    default_args={"on_failure_callback": notify_failure},
    tags=["finance", "reconciliation"],
)
def reconciliation_dag():

    @task()
    def build_weekly_reconciliation() -> Dict[str, Any]:
        context = get_current_context()
        logical_date = context["logical_date"].date()


        # Determine Monday-Sunday calendar week boundaries
        # weekday(): Monday=0, Sunday=6
        week_start_date = logical_date - timedelta(days=logical_date.weekday())
        week_end_date = week_start_date + timedelta(days=6)

        week_start_str = week_start_date.strftime("%Y-%m-%d")
        week_end_str = week_end_date.strftime("%Y-%m-%d")
        logical_date_str = logical_date.strftime("%Y-%m-%d")

        logger.info(
            "Running reconciliation for week: [%s to %s] | Triggered by Logical date: %s",
            week_start_date,
            week_end_str,
            logical_date_str
        )

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        # Consolidated Multi-Source Aggregation Query with Date Backbone
        reconciliation_query = """
            WITH date_backbone AS (
                SELECT generate_series (
                    %s::DATE,
                    LEAST(%s::DATE, %s::DATE),
                    INTERVAL '1 day'
                )::DATE AS calendar_date
            ),
            daily_signups AS (
                SELECT
                    signup_date,
                    COUNT(*) AS signup_count
                FROM customer_signups
                WHERE signup_date BETWEEN %s::DATE AND %s::DATE
                GROUP BY signup_date
            ),
            daily_clean_fx AS (
                SELECT
                    logical_date,
                    jsonb_object_agg(currency_code, rate_to_usd) AS clean_rates
                FROM clean_exchange_rates
                WHERE logical_date BETWEEN %s::DATE AND %s::DATE
                GROUP BY logical_date
            ),
            daily_flagged_fx AS (
                SELECT
                    logical_date,
                    jsonb_agg(jsonb_build_object(
                        'currency', currency_code,
                        'rate', rate_to_usd,
                        'previous_rate', previous_rate_to_usd,
                        'pct_change', percentage_change
                    )) AS flagged_rates,
                    COUNT(*) AS flagged_count
                FROM flagged_exchange_rates
                WHERE logical_date BETWEEN %s::DATE AND %s::DATE
                GROUP BY logical_date
            ),
            daily_settlements AS (
                SELECT 
                    settlement_date,
                    filepath,
                    row_count
                FROM processed_settlement_files
                WHERE settlement_date BETWEEN %s::DATE AND %s::DATE
            )
            SELECT 
                db.calendar_date,
                COALESCE(s.signup_count, 0) AS signup_count,
                cfx.clean_rates,
                ffx.flagged_rates,
                COALESCE(ffx.flagged_count, 0) AS flagged_count,
                st.filepath AS settlement_filepath,
                st.row_count AS settlement_row_count,
                CASE WHEN st.settlement_date IS NOT NULL THEN TRUE ELSE FALSE END AS settlement_processed
            FROM date_backbone db
            LEFT JOIN daily_signups s ON db.calendar_date = s.signup_date
            LEFT JOIN daily_clean_fx cfx ON db.calendar_date = cfx.logical_date
            LEFT JOIN daily_flagged_fx ffx ON db.calendar_date = ffx.logical_date
            LEFT JOIN daily_settlements st ON db.calendar_date = st.settlement_date
            ORDER BY db.calendar_date ASC;
        """

        params = (
            week_start_str,
            week_end_str,
            logical_date_str,  # date_backbone window
            week_start_str,
            week_end_str,  # signups window
            week_start_str,
            week_end_str,  # clean fx window
            week_start_str,
            week_end_str,  # flagged fx window
            week_start_str,
            week_end_str,  # settlements window
        )

        bootstrap_schema("my_postgres_conn") # guarantee all tables in the entire platform exists

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(reconciliation_query, params)
                rows = cursor.fetchall()

        # Aggregation of metric and constructing payload in-memory
        total_signups = 0
        days_settlement_processed = 0
        days_settlement_missing = 0
        total_anomalous_fx_count = 0
        daily_breakdown = []

        for row in rows:
            (
                cal_date,
                signups,
                clean_rates,
                flagged_rates,
                flagged_counts,
                settlement_path,
                settlement_rows,
                is_settled,
            ) = row

            total_signups += signups
            total_anomalous_fx_count += flagged_counts

            if is_settled:
                days_settlement_processed += 1
            else:
                days_settlement_missing += 1

            daily_breakdown.append({
                "date": cal_date.strftime("%Y-%m-%d"),
                "signups": signups,
                "clean_rates": clean_rates or {},
                "flagged_rates": flagged_rates or [],
                "settlement": {
                    "processed": is_settled,
                    "filepath": settlement_path,
                    "row_count": settlement_rows,
                }
            })

        # Status Calculation 
        if logical_date < week_end_date:
            if days_settlement_missing > 0:
                status = "IN_PROGRESS_WITH_GAPS"
            elif total_anomalous_fx_count > 0:
                status = "IN_PROGRESS_WITH_ANOMALIES"
            else:
                status = "IN_PROGRESS"
        else:
            if days_settlement_missing > 0:
                status = "PENDING_AUDIT_GAPS"
            elif total_anomalous_fx_count > 0:
                status = "PENDING_FX_REVIEW"
            else:
                status = "FINAL"

        report_payload = {
            "week_start_date": week_start_str,
            "week_end_date": week_end_str,
            "evaluated_as_of": logical_date_str,
            "status": status,
            "summary": {
                "total_signups": total_signups,
                "days_settlement_processed": days_settlement_processed,
                "days_settlement_missing": days_settlement_missing,
                "anomalous_fx_count": total_anomalous_fx_count,
            },
            "daily_audit": daily_breakdown,
        }

        # Idempotent upsert into destination table
        upsert_report_sql = """
            INSERT INTO weekly_reconciliation_reports (
                week_start_date, week_end_date, last_evaluated_date, generated_at,
                status, total_signups, days_settlement_processed, days_settlement_missing,
                anomalous_fx_count, reconciliation_payload
            )
            VALUES (%s, %s, %s, CURRENT_TIMESTAMP, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (week_start_date)
            DO UPDATE SET
                last_evaluated_date = EXCLUDED.last_evaluated_date,
                generated_at = CURRENT_TIMESTAMP,
                status = EXCLUDED.status,
                total_signups = EXCLUDED.total_signups,
                days_settlement_processed = EXCLUDED.days_settlement_processed,
                days_settlement_missing = EXCLUDED.days_settlement_missing,
                anomalous_fx_count = EXCLUDED.anomalous_fx_count,
                reconciliation_payload = EXCLUDED.reconciliation_payload;
        """

        upsert_values = (
            week_start_str,
            week_end_str,
            logical_date_str,
            status,
            total_signups,
            days_settlement_processed,
            days_settlement_missing,
            total_anomalous_fx_count,
            Json(report_payload),
        )

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(upsert_report_sql, upsert_values)
                conn.commit()

        logger.info(
            "[RECONCILIATION SUCCESS] Week %s Status: %s | Signups: %d | Settled: %d | Missing: %d | Anomalies: %d",
            week_start_str,
            status,
            total_signups,
            days_settlement_processed,
            days_settlement_missing,
            total_anomalous_fx_count,
        )

        return report_payload

    build_weekly_reconciliation()


reconciliation_dag()
