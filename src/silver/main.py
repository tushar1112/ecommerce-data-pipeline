# empty object for unix features (error handle)
import socketserver
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from src.common.bootstrap import init
from src.silver.transformations import process_file


def run():
    logger = None
    try:

        config, logger, spark = init()
        logger.info("***    Bronze layer execution started   ***")
        logger.info("***    Spark session created   ***")

        bronze_path = config["paths"]["Bronze"]
        silver_path = config["paths"]["Silver"]
        reject_path_silver = config["paths"]["Rejected_data_silver"]
        files = config["files"]

        silver_results = {}
        for file_name in files:
            dataset_name = file_name.replace('.csv', '')
            read_path_ = f'{bronze_path}/{dataset_name}'
            silver_path_ = f'{silver_path}/{dataset_name}'
            reject_path_ = f'{reject_path_silver}/{dataset_name}'
            status = process_file(
                spark=spark,
                logger=logger,
                read_path=read_path_,
                silver_path=silver_path_,
                reject_path=reject_path_,
                database_name=dataset_name
            )
            silver_results[dataset_name] = status

        logger.info("Silver layer Processing Summary")
        for dataset, status in silver_results.items():
            if status:
                logger.info(
                    f"{dataset}: SUCCESS"
                )
            else:
                logger.error(
                    f"{dataset}: FAILED"
                )

        if spark:
            spark.stop()
            if logger:
                logger.info("******  spark session stopped  ******")

        logger.info("Silver layer Processing ended")

    except Exception as e:
        logger.error(f" Silver Layer Processing failed : {str(e)}")
        raise

if __name__ == "__main__":
    run()