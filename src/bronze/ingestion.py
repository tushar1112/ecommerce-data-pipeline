from os import truncate

from pyspark.sql import functions as F
from pyspark.sql.functions import *
import os

from src.common.watermark import update_watermark


def read_csv_file(spark,read_path):
    df = (spark.read.option('header','true')
          .option('inferSchema','false')
          .option('quote','"')
          .option('escape','"')
          .option("multiLine", "true")
          .csv(read_path))
    return df

def add_audit_columns(df,file_name,batch_id):
    # adding metadata columns(filename, timestamp) to given dataframe. used to debug source file and modified date
    df = (
        df.withColumn('source_table',lit(file_name))
            .withColumn('ingestion_timestamp',current_timestamp())
            .withColumn('ingestion_date',current_date())
            .withColumn('batch_id',lit(batch_id))

    )
    return df

def write_bronze(df,write_path,mode):
    df.write.mode(mode).partitionBy('ingestion_date').parquet(write_path)


def full_ingestion(spark,logger,read_path,write_path,file_name,batch_id,mode):

    logger.info(f" Full load Ingestion started for raw {file_name} ")

    df = read_csv_file(spark,read_path)
    logger.info(f"Raw {file_name} rows count : {df.count()}")

    df= add_audit_columns(df,file_name,batch_id)
    df = df.withColumn('operation_type',lit('Full Load'))
    write_bronze(df,write_path,mode)

    print(f'Ingestion of {file_name} completed')
    logger.info(f"***** Ingestion completed for {file_name} *******")

def incremental_ingestion(spark,logger,read_path,write_path,file_name,batch_id,mode,watermark_path):
    try:
        logger.info(f" Incremental ingestion started for {file_name} ")
        df = read_csv_file(spark, read_path)
        logger.info(f"Raw {file_name} rows count : {df.count()}")

        df = add_audit_columns(df, file_name, batch_id)
        write_bronze(df, write_path, mode)
        update_watermark(spark, watermark_path, file_name.replace('.csv', ''), 'bronze', batch_id, 'SUCCESS')

        print(f'Incremental Ingestion completed for {file_name}')
        logger.info(f"***** Incremental Ingestion completed for {file_name} *******")
    except Exception as e:
        update_watermark(spark, watermark_path, file_name.replace('.csv', ''), 'bronze', batch_id, 'FAILED')
        raise





