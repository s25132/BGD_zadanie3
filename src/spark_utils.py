import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "bgd-medallion-pipeline") -> SparkSession:
    spark_master = os.getenv("SPARK_MASTER", "local[*]")

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
    )
    return spark