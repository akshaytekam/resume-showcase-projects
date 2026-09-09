import time
import logging

from src.config import (
    MAX_RETRIES,
    RETRY_INTERVAL_SECONDS
)


logger = logging.getLogger(__name__)


def execute_with_retry(
    operation,
    operation_name: str,
    max_retries: int = MAX_RETRIES,
    retry_interval: int = RETRY_INTERVAL_SECONDS
):
    """
    Execute an operation with retry support.

    Parameters
    ----------
    operation:
        Function that performs the actual processing.

    operation_name:
        Name used in logs.

    max_retries:
        Number of retry attempts.

    retry_interval:
        Seconds to wait between retries.
    """

    attempt = 0

    while attempt <= max_retries:

        try:

            logger.info(
                f"Starting {operation_name}. "
                f"Attempt: {attempt + 1}"
            )

            result = operation()

            logger.info(
                f"{operation_name} completed successfully."
            )

            return result

        except Exception as exc:

            attempt += 1

            logger.error(
                f"{operation_name} failed. "
                f"Attempt: {attempt}. "
                f"Error: {str(exc)}"
            )

            if attempt > max_retries:

                logger.error(
                    f"{operation_name} failed after "
                    f"{max_retries} retries."
                )

                raise

            logger.info(
                f"Retrying {operation_name} "
                f"in {retry_interval} seconds..."
            )

            time.sleep(retry_interval)

from datetime import datetime

from audit import write_audit_record


try:

    execute_with_retry(
        operation=process_bronze,
        operation_name="Bronze ADAS Load"
    )

except Exception as exc:

    write_audit_record(
        spark=spark,
        pipeline_name="adas_dlt_pipeline",
        environment="dev",
        layer="bronze",
        table_name="bronze_adas_events",
        batch_id="batch_20260909_001",
        start_time=start_time,
        end_time=datetime.now(),
        records_read=0,
        records_written=0,
        records_rejected=0,
        status="FAILED",
        error_message=str(exc)
    )

    raise
