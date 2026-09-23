"""Shared Spark session configured for Delta Lake.

Standard path uses `configure_spark_with_delta_pip` (resolves the Delta
package from Maven). If DELTA_JARS is set (comma-separated local jar paths),
those jars are used directly instead — handy for offline/air-gapped runs.
"""
import os

from pyspark.sql import SparkSession


def get_spark(app_name="medallion-demo", warehouse_dir=None):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    )
    if warehouse_dir:
        builder = builder.config("spark.sql.warehouse.dir", warehouse_dir)
    jars = os.environ.get("DELTA_JARS")
    if jars:
        builder = builder.config("spark.jars", jars)
        return builder.getOrCreate()
    from delta import configure_spark_with_delta_pip
    return configure_spark_with_delta_pip(builder).getOrCreate()
