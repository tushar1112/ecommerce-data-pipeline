from pyspark.sql import functions as F
from pyspark.sql.functions import *

def read_csv_file(spark,read_path):
    df = (spark.read.options(header='true',inferschema='true')
          .csv(read_path))
    return df

def add_audit_columns(df,file_name):
    # adding metadata columns(filename, timestamp) to given dataframe. used to debug source file and modified date
    df = (
        df.withColumn('source_file',lit(file_name))
            .withColumn('time_stamp',current_timestamp())
    )
    return df

def write_bronze(df,write_path):
    # writing df to bronze layer location as parquet(faster read, compressed data).
    (df.write.mode('overwrite').parquet(write_path))

def full_ingestion(spark,logger,read_path,write_path,file_name):
    logger.info(f" Data ingestion stated from {file_name} into {write_path} layer")

    df = read_csv_file(spark,read_path)
    df = add_audit_columns(df,file_name)
    write_bronze(df,write_path)

    print(f'injestion of {file_name} completed')
    logger.info(f"***** Ingestion completed for {file_name} *******")