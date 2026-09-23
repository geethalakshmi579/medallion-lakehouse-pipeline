"""GOLD: business-ready aggregates.

- gold_daily_revenue: revenue / orders / AOV per day x category
- gold_customer_ltv: lifetime value, order count, first/last order per customer
"""
from pyspark.sql import functions as F
from spark_session import get_spark

SILVER = "warehouse/silver_orders"
DIM = "warehouse/silver_dim_customers"
GOLD_REV = "warehouse/gold_daily_revenue"
GOLD_LTV = "warehouse/gold_customer_ltv"


def run(spark=None):
    spark = spark or get_spark()
    orders = spark.read.format("delta").load(SILVER)
    customers = spark.read.format("delta").load(DIM).filter("is_current = true")

    daily = (
        orders.withColumn("order_date", F.to_date("order_ts"))
        .groupBy("order_date", "category")
        .agg(F.round(F.sum("line_total"), 2).alias("revenue"),
             F.count("*").alias("orders"),
             F.round(F.avg("line_total"), 2).alias("avg_order_value"))
    )
    (daily.write.format("delta").mode("overwrite").save(GOLD_REV))

    ltv = (
        orders.join(customers.select("customer_id", "country"), "customer_id")
        .groupBy("customer_id", "country")
        .agg(F.round(F.sum("line_total"), 2).alias("lifetime_value"),
             F.count("*").alias("n_orders"),
             F.min("order_ts").alias("first_order_ts"),
             F.max("order_ts").alias("last_order_ts"))
    )
    (ltv.write.format("delta").mode("overwrite").save(GOLD_LTV))

    print(f"[gold] {daily.count()} daily rows -> {GOLD_REV}; "
          f"{ltv.count()} customers -> {GOLD_LTV}")
    print("[gold] top categories by revenue:")
    daily.groupBy("category").agg(F.round(F.sum("revenue"), 2).alias("revenue")) \
         .orderBy(F.desc("revenue")).show(truncate=False)
    return daily.count(), ltv.count()


if __name__ == "__main__":
    run()
