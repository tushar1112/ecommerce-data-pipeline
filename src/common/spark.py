import socketserver
import os

# This tells Python: "If you look for Unix features, just find this empty object instead."
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from pyspark.sql import SparkSession

def create_spark_session(config):

    spark=(
        SparkSession.builder
        .appName(config["app_name"])
        .master(config["spark"]["master"])
        .config("spark.sql.shuffle.partitions", config["spark"]["shuffle_partitions"])
        .config("spark.driver.memory", config["spark"]["driver_memory"])
        .config("spark.default.parallelism", "8")
        .config("spark.executor.memory", "4g")
        .config(
            "spark.sql.execution.arrow.pyspark.enabled",
            "true"
        )
        .config(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )
        .config(
            "spark.sql.execution.pyspark.udf.faulthandler.enabled",
            'true'
        ).config(
            "spark.python.worker.faulthandler.enabled",
            'true'
        ).config("spark.driver.maxResultSize", "2g")  # Add this to prevent Java thread drops
        .config("spark.executor.memoryOverhead", "1g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config["spark"]["log_level"])

    return spark
