"""SILVER: cleanse bronze events.

- Deduplicate on order_id (keep the latest ingested copy)
- Quarantine invalid rows (quantity <= 0, unknown customer) with a reason
- Cast to proper types
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from spark_session import get_spark

BRONZE = "warehouse/bronze_orders"
SILVER = "warehouse/silver_orders"
QUARANTINE = "warehouse/quarantine_orders"
CUSTOMERS = "data/raw/customers/customers_day1.csv"


def run(spark=None):
    spark = spark or get_spark()
    bronze = spark.read.format("delta").load(BRONZE)
    known_customers = {r.customer_id for r in
                       spark.read.csv(CUSTOMERS, header=True).select("customer_id").distinct().collect()}

    deduped = bronze.withColumn(
        "_rn",
        F.row_number().over(Window.partitionBy("order_id").orderBy(F.desc("_ingest_ts"))),
    ).filter("_rn = 1").drop("_rn")

    bad = deduped.filter(
        (F.col("quantity") <= 0)
        | (~F.col("customer_id").isin(known_customers))
        | F.col("order_id").isNull()
    )
    bad = bad.withColumn(
        "quarantine_reason",
        F.when(F.col("quantity") <= 0, F.lit("non_positive_quantity"))
        .when(~F.col("customer_id").isin(known_customers), F.lit("unknown_customer"))
        .otherwise(F.lit("null_order_id")),
    )
    (bad.write.format("delta").mode("overwrite").save(QUARANTINE))

    silver = (
        deduped.filter(F.col("customer_id").isin(known_customers))
        .filter("quantity > 0 AND order_id IS NOT NULL")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("product_id"),
            F.col("category"),
            F.col("quantity").cast("int").alias("quantity"),
            F.col("unit_price").cast("double").alias("unit_price"),
            (F.col("quantity") * F.col("unit_price")).alias("line_total"),
            F.to_timestamp("order_ts").alias("order_ts"),
            F.col("channel"),
            F.col("promo_code"),
        )
    )
    (silver.write.format("delta").mode("overwrite")
     .option("mergeSchema", "true").save(SILVER))

    n_silver = spark.read.format("delta").load(SILVER).count()
    n_bad = spark.read.format("delta").load(QUARANTINE).count()
    print(f"[silver] {n_silver} clean orders -> {SILVER}; {n_bad} quarantined -> {QUARANTINE}")
    return n_silver, n_bad


if __name__ == "__main__":
    run()
