# empty object for unix features (error handle)
import socketserver
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from src.common.bootstrap import init
from src.bronze.ingestion import full_ingestion

def run():
    logger = None

    try:
        config, logger, spark = init()
        logger.info("***    Bronze layer execution started   ***")
        logger.info("***    Spark session created   ***")

        raw_path = config["paths"]["raw"]
        bronze_path = config["paths"]["Bronze"]
        files = config["files"]

        for file_name in files:
            dataset_name = file_name.replace('.csv', '')
            read_path_ = f'{raw_path}/{file_name}'
            bronze_path_ = f'{bronze_path}/{dataset_name}'
            full_ingestion(
                spark=spark,
                logger=logger,
                read_path=read_path_,
                write_path=bronze_path_,
                file_name=file_name
            )
            logger.info(f"---   data ingestion completed for {dataset_name}  ---")
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

