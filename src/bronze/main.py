# empty object for unix features (error handle)

import socketserver
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object
#**************************************************************************************

from src.common.bootstrap import init
from src.bronze.ingestion import full_ingestion, incremental_ingestion
from pyspark.sql.functions import current_timestamp,col
from datetime import datetime
from src.common.watermark import get_last_processed_batch

def run():
    logger = None

    try:
        config, logger, spark = init()
        logger.info("***    Bronze layer execution started   ***")
        logger.info("***    Spark session created   ***")

        raw_full = config["paths"]["raw_full"]
        raw_inc = config["paths"]["raw_inc"]
        bronze_path = config["paths"]["bronze"]
        files = config["files"]
        files_inc = config["files_inc"]
        watermark_path = config["paths"]["watermark"]
        batch_id = datetime.now().strftime("BATCH_%Y%m%d_%H%M%S")

        load_type = config["load_type"]
        if load_type == 'FULL_LOAD':
            for file_name in files:
                dataset_name = file_name.replace('.csv', '')
                read_path_full = f'{raw_full}/{file_name}'
                bronze_path_ = f'{bronze_path}/{dataset_name}'
                full_ingestion(
                    spark=spark,
                    logger=logger,
                    read_path=read_path_full,
                    write_path=bronze_path_,
                    file_name=file_name,
                    batch_id=batch_id,
                    mode='overwrite'
                )

        else:
            for dataset_name in files_inc.keys():
                read_path_incremental = f'{raw_inc}/{dataset_name}'
                bronze_path_ = f'{bronze_path}/{dataset_name}'
                incremental_ingestion(
                    spark=spark,
                    logger=logger,
                    read_path=read_path_incremental,
                    write_path=bronze_path_,
                    file_name=f'{dataset_name}.csv',
                    batch_id=batch_id,
                    mode='append',
                    watermark_path=watermark_path
                )

            watermark_df = spark.read.parquet(watermark_path)
            watermark_df.orderBy(col('last_batch_id').desc()).show()
            print(watermark_df.count())

        if spark:
            spark.stop()
            if logger:
                logger.info("******  spark session stopped  ******")
        logger.info('*****  Bronze layer executed successfully    *****')

    except Exception as e:
        logger.error(f"Bronze layer execution failed : {str(e)}")
        raise

if __name__ == "__main__":
    run()

