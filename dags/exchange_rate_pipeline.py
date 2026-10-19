import json
import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List

import requests
from airflow.decorators import task, dag
from airflow.operators.python import get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from utils import notify_failure
from utils import bootstrap_schema


logger = logging.getLogger(__name__)

@dag(
    dag_id="exchange_rates_etl_pipeline",
    start_date=datetime(2026, 7, 26),
    schedule="@daily",
    catchup=False,
    default_args={"on_failure_callback": notify_failure},
    tags=["finance", "fx", "api"],
)
def exchange_rates_etl_pipeline():

    @task()
    def extract_raw_exchange_rates() -> dict:
        """Fetches raw exchange rates from API configured via Airflow Variable,

        performs semantic validation, and appends the exact JSON response into 
        an immutable audit table in Postgres.

        Returns:
            dict: Contains 'raw_id' (exact DB row ID) and 'logical_date'
            string.
        """
        # Fetch configurations from Airflow Variables inside task execution context
        exchange_rates_api_url = Variable.get("EXCHANGE_RATE_API_URL")
        required_currencies = Variable.get("REQUIRED_CURRENCIES", deserialize_json=True)

        context = get_current_context()
        logical_date_str = context["logical_date"].strftime("%Y-%m-%d")

        logger.info("Fetching exchange rate for execution date: %s", logical_date_str)

        # HTTP Network Fetch
        response = requests.get(exchange_rates_api_url, timeout=10)
        response.raise_for_status()

        # Safe JSON parsing
        try:
            raw_json_data = response.json()
        except ValueError as e:
            logger.error("Failed to decode JSON. Response text: %s", response.text[:200])
            raise ValueError(f"API returned 200 OK, but payload is not valid JSON: {e}")

        # Semantic Validation 
        if raw_json_data.get("result") != "success":
            raise ValueError(
                f"API returned 200 OK, but reported error: {raw_json_data.get('error-type')}"
            )

        rates = raw_json_data.get("rates", {}) 
        missing_currencies = []
        invalid_currencies = []
        valid_rates = {}

        # Explicitly inspect each target currency to catch partial payload drops
        for currency in required_currencies:
            if currency not in rates:
                missing_currencies.append(currency)
            else:
                rate_val = rates[currency]
                if not isinstance(rate_val, (int, float)) or rate_val <= 0:
                    invalid_currencies.append((currency, rate_val))
                else:
                    valid_rates[currency] = float(rate_val)

        # Flag partial data anomalies prominently in logs without crashing execution
        if missing_currencies or invalid_currencies:
            logger.warning(
                "PARTIAL DATA WARNING for logical date %s!\n"
                "Missing currencies: %s\n"
                "Invalid rate values: %s\n"
                "Proceeding with available rates: %s\n",
                logical_date_str,
                missing_currencies if missing_currencies else "None",
                invalid_currencies if invalid_currencies else "None",
                list(valid_rates.keys()),
            )

        # Hard stop only if ALL target currencies fail
        if not valid_rates:
            raise ValueError(
                f"Extraction failed: None of the target currencies {required_currencies} "
                f"were present or valid in the API response."
            )

        # Preserve Untouched Payload in Postgres Raw Landing Table (Audit Trail)
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        insert_raw_sql = """
            INSERT INTO raw_exchange_rates (logical_date, fetched_at, source_url, raw_response)
            VALUES (%s, %s, %s, %s)
            RETURNING id
        """

        values = (
            logical_date_str,
            datetime.utcnow(), 
            exchange_rates_api_url, 
            json.dumps(raw_json_data),
        )

        # Guarantee entire table schema exists safely under advisory lock
        bootstrap_schema("my_postgres_conn")

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(insert_raw_sql, values,)
                inserted_id = cursor.fetchone()[0]
                conn.commit()

        logger.info(
            "Successfully recorded raw API response [Row ID: %d] for %s",
            inserted_id,
            logical_date_str,
        )

        # Pass exact record ID reference to eliminate downstream via Xcom
        return {"raw_id": inserted_id, "logical_date": logical_date_str}

    @task()
    def transform_exchange_rates(extract_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Reads raw JSON payload from 'raw_exchange_rates' by exact row ID, 
        extracts and normalizes target currencies into a tall/narrow structure.

        Args:
            extract_metadata (dict): Contains 'raw_id' (int) and 'logical_date'
            (str) from extract.

        Returns:
            list[dict]: List of normalized rate records ready for loading.
        """
        raw_id = extract_metadata["raw_id"]
        logical_date_str = extract_metadata["logical_date"]

        # Fetch target currencies from Airflow Variable
        required_currencies = Variable.get("REQUIRED_CURRENCIES", deserialize_json=True)

        logger.info("Starting transformation for raw_id: %d (Date: %s)", raw_id, logical_date_str)

        # Fetch extract raw JSON payload using primary keys
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        select_raw_sql = "SELECT raw_response FROM raw_exchange_rates WHERE id = %s;"

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(select_raw_sql, (raw_id,))
                result = cursor.fetchone()
                if not result:
                    raise ValueError(f"No raw payload found in 'raw_exchange_rates' for id: {raw_id}")

                # Handles both DB JSONB types and raw JSON string fallbacks safely
                raw_payload = result[0]
                raw_json_data = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload

        # Extract rates from dictionary
        rates = raw_json_data.get("rates", {})
        if not isinstance(rates, dict):
            raise KeyError(f"Raw payload for id {raw_id} is missing a valid 'rates' dictionary")

        cleaned_records = []
        missing_or_invalid = []

        # Shape into tall/narrow format (1 row per valid currency)
        for currency in required_currencies:
            rate_val = rates.get(currency)

            # Validate it returns a positive number
            if isinstance(rate_val, (int, float)) and rate_val > 0:
                cleaned_records.append({
                    "logical_date": logical_date_str,
                    "currency_code": currency,
                    "rate_to_usd": float(rate_val),
                    "source_raw_id": raw_id
                })
            else:
                missing_or_invalid.append((currency, rate_val))

        # Log warnings for missing currencies without failing execution
        if missing_or_invalid:
            logger.warning(
                "Transformation warning for raw_id %d: Skipped missing/invalid currencies: %s",
                raw_id,
                missing_or_invalid
            )

        # Hard failure only if transformation yields 0 usable records
        if not cleaned_records:
            raise ValueError(
                f"Transformation failed for raw_id {raw_id}: None of {required_currencies} "
                f"could be parsed into valid rates."
            )

        logger.info(
            "Successfully transformed %d rates for raw_id %d: %s",
            len(cleaned_records),
            raw_id,
            [r["currency_code"] for r in cleaned_records]
        )

        # pass list of dicts directly in memory via Xcom
        return cleaned_records

    # BRANCH TASK: Evaluate Anomaly Thresholds and Route Execution
    @task()
    def check_for_anomalies(
        cleaned_records: List[Dict[str, Any]]
    )-> Dict[str, Any]:
        """Queries clean_exchange_rates for yesterday's baseline, compares percent

        changes against ANOMALY_THRESHOLD_PERCENTAGE, and returns XCom metadata
        alongside the winning target task_id.
        """
        # Fetch threshold from Airflow Variable
        threshold = float(
            Variable.get("ANOMALY_THRESHOLD_PERCENTAGE", default_var="0.15")
        )

        context = get_current_context()
        logical_date = context["logical_date"].date()
        logical_date_str = logical_date.strftime("%Y-%m-%d")
        yesterday_str = (logical_date - timedelta(days=1)).strftime("%Y-%m-%d")

        logger.info(
            "Evaluating rates for %s against baseline %s with threshold %.2f%%...",
            logical_date_str,
            yesterday_str,
            threshold * 100,
        )

        # SQL query fetching all baseline rates for yesterday
        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")
        baseline_sql = """
            SELECT currency_code, rate_to_usd
            FROM clean_exchange_rates
            WHERE logical_date = %s;
        """

        yesterday_rates = {}
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(baseline_sql, (yesterday_str,))
                rows = cursor.fetchall()
                for currency_code, rate in rows:
                    yesterday_rates[currency_code] = float(rate)

        # if no historical baseline exists, route as NORMAL
        if not yesterday_rates:
            logger.info(
                "No previous day records found for %s (cold start). Routing to normal load.",
                yesterday_str
            )
            return {
                "target_task" : "load_clean_exchange_rates",
                "enriched_records" : [],
            }

        is_anomalous = False
        enriched_records = []

        for record in cleaned_records:
            currency = record["currency_code"]
            current_rate = record["rate_to_usd"]
            prev_rate = yesterday_rates.get(currency)

            pct_change = None
            if prev_rate and prev_rate > 0:
                pct_change = abs(current_rate - prev_rate) / prev_rate
                if pct_change > threshold:
                    is_anomalous = True
                    logger.warning(
                        "ANOMALY DETECTED for %s on %s: Yesterday=%.6f, Today=%.6f (%.2f%% change > %.2f%% threshold)",
                        currency,
                        logical_date_str,
                        prev_rate,
                        current_rate,
                        pct_change * 100,
                        threshold * 100,
                    )

            enriched_records.append({
                "logical_date" : record["logical_date"],
                "currency_code" : currency,
                "rate_to_usd" : current_rate,
                "previous_rate_to_usd" : prev_rate,
                "percentage_change" : (
                    round(pct_change, 4) if pct_change is not None else None
                ),
                "source_raw_id" : record["source_raw_id"],
            })

        # Return target task ID for branching, embedding enriched payload in XCom
        target_task = (
            "flag_anomalous_rates" if is_anomalous else "load_clean_exchange_rates"
        )
        return {
            "target_task": target_task,
            "enriched_records": enriched_records,
        }
        
    # Custom wrapper ensuring TaskFlow extracts the target_task string for branching
    @task.branch()
    def route_decision(branch_payload: Dict[str, Any]) -> str:
        return branch_payload["target_task"]

    # PATH A: Normal Load Task
    @task()
    def load_clean_exchange_rates(clean_records: List[Dict[str, Any]]) -> None:
        """Loads normalized currency records into 'clean_exchange_rates'.
        """
        if not clean_records:
            logger.warning(
                "No clean records recieved in load_clean_exchange_rates task. Skipping load."
            )
            return

        logger.info(
            "Preparing atomic bulk upsert for %d clean rate record(s)...",
            len(clean_records),
        )

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        # execute_values dynamically formats (%s, %s, %s, %s) for all rows into a single multi-row INSERT
        upsert_sql = """
            INSERT INTO clean_exchange_rates (
                logical_date,
                currency_code,
                rate_to_usd,
                source_raw_id
            )
            VALUES %s
            ON CONFLICT (logical_date, currency_code)
            DO UPDATE SET
                rate_to_usd = EXCLUDED.rate_to_usd,
                source_raw_id = EXCLUDED.source_raw_id,
                created_at = CURRENT_TIMESTAMP
        """

        # Transform list of dicts into tuple parameter list expected by execute_values
        records_to_insert = [
            (
                rec["logical_date"],
                rec["currency_code"],
                rec["rate_to_usd"],
                rec["source_raw_id"],
            )
            for rec in clean_records
        ]

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, upsert_sql, records_to_insert)
                conn.commit()

        logger.info(
            "Successfully upserted %d record(s) into 'clean_exchange_rates'.",
            len(clean_records),
        )

    # PATH B: Flagged / Anomalous Load Task
    @task()
    def flag_anomalous_rates(branch_payload: Dict[str, Any]) -> None:
        """Saves anomalous records into Postgres with comparison metadata
        AND outputs the business anomaly alert payload.
        """
        context = get_current_context()
        logical_date_str = context["logical_date"].strftime("%Y-%m-%d")
        
        enriched_records = branch_payload.get("enriched_records", [])
        if not enriched_records:
            logger.warning(
                "No enriched records recieved in flag_anomalous_rates task."
            )
            return

        logger.info(
            "Recording %d flagged anomalous record(s) for manual review...",
            len(enriched_records),
        )

        pg_hook = PostgresHook(postgres_conn_id="my_postgres_conn")

        upsert_flagged_sql = """
            INSERT INTO flagged_exchange_rates (
                logical_date, currency_code, rate_to_usd, previous_rate_to_usd,
                percentage_change, source_raw_id)
            VALUES %s
            ON CONFLICT (logical_date, currency_code)
            DO UPDATE SET
                rate_to_usd = EXCLUDED.rate_to_usd,
                previous_rate_to_usd = EXCLUDED.previous_rate_to_usd,
                percentage_change = EXCLUDED.percentage_change,
                source_raw_id = EXCLUDED.source_raw_id,
                flagged_at = CURRENT_TIMESTAMP
        """

        records_to_insert = [
            (
                rec["logical_date"],
                rec["currency_code"],
                rec["rate_to_usd"],
                rec["previous_rate_to_usd"],
                rec["percentage_change"],
                rec["source_raw_id"],
            )
            for rec in enriched_records
        ]

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor, upsert_flagged_sql, records_to_insert)
                conn.commit()

        # Business Anomaly Notification Log (fires AFTER successful DB persistence)
        logger.warning(
            "\n===================================================================\n"
            "[EMAIL WOULD SEND] To: finance-alerts@paynaija.com |\n"
            "Subject: Exchange Rate Anomaly Detected (%s) |\n"
            "Body: %d currencies flagged for manual review: %s\n"
            "===================================================================",
            logical_date_str,
            len(enriched_records),
            [(r["currency_code"], r["percentage_change"]) for r in enriched_records],
        )

    # RECONVERGING TASK: Pipeline completion log
    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def log_pipeline_completion() -> None:
        """Executes regardless of which load branch was taken, confirming run completion.
        """
        logger.info(
            "Exchange Rates ETL pipeline completed processing successfully."
        )
    
    # Pipeline Task Execution flow
    raw_meta = extract_raw_exchange_rates()
    cleaned = transform_exchange_rates(raw_meta)

    # Branch calculation and routing
    branch_data = check_for_anomalies(cleaned)
    selected_route = route_decision(branch_data)

    # Downstream branches
    normal_load = load_clean_exchange_rates(cleaned)
    flagged_load = flag_anomalous_rates(branch_data)

    # Wire branching dependencies
    selected_route >> normal_load
    selected_route >> flagged_load

    # Reconverge downstream branches into completion task
    [normal_load, flagged_load] >> log_pipeline_completion()

# Instantiate the DAG
exchange_rates_etl_pipeline()   