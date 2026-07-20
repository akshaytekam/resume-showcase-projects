import pandas as pd

from pathlib import Path

from utils.logger import logger

from validation.validation_result import ValidationResult


def load_csv(file_path: str) -> pd.DataFrame:
    """
    Load CSV safely.
    """

    logger.info(f"Loading file : {file_path}")

    return pd.read_csv(file_path)


def save_bad_records(df, file_name):
    """
    Save rejected records.
    """

    reject_path = Path("validation/reports/rejected")

    reject_path.mkdir(parents=True, exist_ok=True)

    output = reject_path / file_name

    df.to_csv(output, index=False)

    logger.info(f"Rejected records saved -> {output}")


def validation_success(
        validation_name,
        total_records,
        message):

    logger.info(message)

    return ValidationResult(

        validation_name=validation_name,

        status="SUCCESS",

        total_records=total_records,

        failed_records=0,

        message=message

    )


def validation_failure(

        validation_name,

        total_records,

        failed_records,

        failed_df,

        message):

    save_bad_records(

        failed_df,

        validation_name.lower().replace(" ", "_") + ".csv"

    )

    logger.error(message)

    return ValidationResult(

        validation_name=validation_name,

        status="FAILED",

        total_records=total_records,

        failed_records=failed_records,

        message=message,

        failed_rows=list(failed_df.index)

    )
