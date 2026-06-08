from pyspark.sql.functions import *
from pyspark.sql.types import *

def read_watermark(spark,watermark_path):
    try:
        return spark.read.parquet(watermark_path)
    except:
        schema = StructType([
            StructField("table_name", StringType(), True),
            StructField("layer", StringType(), True),
            StructField("last_batch_id", StringType(), True),
            StructField("last_processed_timestamp", TimestampType(), True),
            StructField("status", StringType(), True),
            StructField("updated_at", TimestampType(), True),
        ])
        return spark.createDataFrame([], schema)

def get_last_processed_batch(spark, watermark_path,table_name,layer):
    watermark_df = read_watermark(spark, watermark_path)
    row = (
        watermark_df.filter((col('table_name')==table_name) & (col('layer')==layer))
        .orderBy('last_processed_timestamp')
        .first()
    )
    return row["last_batch_id"] if row else None

def update_watermark(spark, watermark_path, table_name, layer, batch_id, status="SUCCESS"):
    data = [(table_name, layer, batch_id, status)]

    print(data)

    df = spark.createDataFrame(
        data,
        ["table_name", "layer", "last_batch_id", "status"]
    )

    df = (
        df.withColumn("last_processed_timestamp", current_timestamp())
          .withColumn("updated_at", current_timestamp())
    )

    df.write.mode("append").parquet(watermark_path)