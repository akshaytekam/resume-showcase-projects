"""
===============================================================================

File Name   : schema_validation.py

Description :
Enterprise Schema Validation Module.

Performs:

• Required Column Validation
• Missing Column Validation
• Unexpected Column Validation
• Data Type Validation
• Nullable Validation
• Primary Key Validation

Author      : Enterprise Data Engineering Team

Version     : 2.0

===============================================================================
"""

from pyspark.sql.types import (
    StringType,
    IntegerType,
    DoubleType,
    DateType
)

from validation.validation_config import (
    schema
)

from validation.validation_utils import (
    build_validation_result
)


class SchemaValidation:

    """
    Validates DataFrame schema.
    """

    def __init__(self, dataframe):

        self.df = dataframe

        self.columns = dataframe.columns

    # =========================================================================
    # Required Columns
    # =========================================================================

    def validate_required_columns(self):

        missing = [

            column

            for column in schema.REQUIRED_COLUMNS

            if column not in self.columns

        ]

        if missing:

            raise Exception(

                f"Missing required columns : {missing}"

            )

    # =========================================================================
    # Unexpected Columns
    # =========================================================================

    def validate_unexpected_columns(self):

        unexpected = [

            column

            for column in self.columns

            if column not in schema.REQUIRED_COLUMNS

        ]

        if unexpected:

            raise Exception(

                f"Unexpected columns : {unexpected}"

            )

    # =========================================================================
    # Primary Key Validation
    # =========================================================================

    def validate_primary_key(self):

        for column in schema.PRIMARY_KEY:

            if column not in self.columns:

                raise Exception(

                    f"Primary key missing : {column}"

                )

    # =========================================================================
    # Data Type Validation
    # =========================================================================

    def validate_data_types(self):

        expected_types = {

            "sale_id": StringType,

            "store_id": StringType,

            "customer_id": StringType,

            "product_id": StringType,

            "sale_date": DateType,

            "quantity": IntegerType,

            "price": DoubleType,

            "sales_amount": DoubleType

        }

        dataframe_schema = dict(self.df.dtypes)

        for column, expected_type in expected_types.items():

            actual_type = dataframe_schema.get(column)

            if actual_type is None:

                continue

            expected = expected_type().simpleString()

            if actual_type != expected:

                raise Exception(

                    f"{column} datatype mismatch "

                    f"Expected={expected} "

                    f"Actual={actual_type}"

                )

    # =========================================================================
    # Nullable Validation
    # =========================================================================

    def validate_nullable(self):

        nullable_columns = [

            field.name

            for field in self.df.schema.fields

            if field.nullable

        ]

        mandatory = [

            "sale_id",

            "store_id",

            "customer_id",

            "product_id",

            "sale_date"

        ]

        invalid = [

            column

            for column in mandatory

            if column in nullable_columns

        ]

        if invalid:

            raise Exception(

                f"Mandatory columns nullable : {invalid}"

            )

    # =========================================================================
    # Main Validation
    # =========================================================================

    def validate(self):

        self.validate_required_columns()

        self.validate_unexpected_columns()

        self.validate_primary_key()

        self.validate_data_types()

        self.validate_nullable()

        return build_validation_result(

            validation_name="Schema Validation",

            status="SUCCESS",

            records_checked=self.df.count(),

            failed_records=0,

            execution_time=0,

            message="Schema validation successful."

        )
