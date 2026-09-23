"""SILVER (dimension): SCD Type 2 merge for the customer dimension.

Each run merges one raw customer snapshot. Changed attributes close the old
row (effective_to / is_current=false) and open a new current row; brand-new
customers are inserted. Demonstrates Delta Lake MERGE INTO.
"""
from delta.tables import DeltaTable
from pyspark.sql import functions as F
from spark_session import get_spark

DIM = "warehouse/silver_dim_customers"
TRACKED = ["name", "email", "country"]


def _snapshot(spark, path):
    return (
        spark.read.csv(path, header=True)
        .select("customer_id", "name", "email", "country",
                F.to_date("signup_date").alias("signup_date"))
        .withColumn("effective_from", F.current_date())
        .withColumn("effective_to", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
    )


def merge_snapshot(spark, snapshot_path):
    snap = _snapshot(spark, snapshot_path)
    if DeltaTable.isDeltaTable(spark, DIM):
        dim = DeltaTable.forPath(spark, DIM)
        current = dim.toDF().filter("is_current = true")

        changed = (
            snap.alias("s").join(current.alias("c"), "customer_id")
            .where(" OR ".join(f"s.{a} <> c.{a}" for a in TRACKED))
            .select("s.customer_id")
        )
        # close changed rows
        dim.alias("d").merge(
            changed.alias("ch"), "d.customer_id = ch.customer_id AND d.is_current = true"
        ).whenMatchedUpdate(set={
            "effective_to": F.current_date(), "is_current": F.lit(False)}).execute()
        # insert new versions of changed rows
        new_versions = snap.join(changed, "customer_id")
        (new_versions.write.format("delta").mode("append").save(DIM))
        # insert brand-new customers
        new_customers = snap.join(current.select("customer_id"), "customer_id", "left_anti")
        (new_customers.write.format("delta").mode("append").save(DIM))
    else:
        (snap.write.format("delta").mode("overwrite").save(DIM))

    df = spark.read.format("delta").load(DIM)
    n_current = df.filter("is_current = true").count()
    n_hist = df.filter("is_current = false").count()
    print(f"[scd2] {snapshot_path}: {n_current} current, {n_hist} historical -> {DIM}")
    return n_current, n_hist


def run(spark=None):
    spark = spark or get_spark()
    merge_snapshot(spark, "data/raw/customers/customers_day1.csv")  # initial load
    return merge_snapshot(spark, "data/raw/customers/customers_day2.csv")  # changes


if __name__ == "__main__":
    run()
