"""BRONZE: land raw JSON events as a Delta table, preserving everything.

Schema drift (the mid-stream `promo_code` field) is absorbed via mergeSchema.
"""
from pyspark.sql import functions as F
from spark_session import get_spark

RAW_ORDERS = "data/raw/orders"
BRONZE = "warehouse/bronze_orders"


def run(spark=None):
    spark = spark or get_spark()
    bronze = (
        spark.read.json(RAW_ORDERS)
        .withColumn("_ingest_ts", F.current_timestamp())
        .withColumn("_source_file", F.input_file_name())
    )
    (bronze.write.format("delta").mode("overwrite")
     .option("mergeSchema", "true").save(BRONZE))
    n = spark.read.format("delta").load(BRONZE).count()
    print(f"[bronze] {n} raw events -> {BRONZE}")
    return n


if __name__ == "__main__":
    run()
