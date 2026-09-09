# ============================================================
# adas_vehicle_cdc.py
# ADAS Vehicle CDC Source
# ============================================================

from pyspark import pipelines as dp
from pyspark.sql.functions import col


@dp.table(
    name="adas_vehicle_cdc"
)
def adas_vehicle_cdc():

    return (
        spark.readStream
        .format("cloudFiles")
        .option(
            "cloudFiles.format",
            "json"
        )
        .load(
            "/Volumes/adas_catalog/dev/adas_volume/raw/vehicle_cdc/"
        )
    )
