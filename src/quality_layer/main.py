# empty object for unix features (error handle)
import socketserver
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from src.common.bootstrap import init
from src.quality_layer.data_quality import run_quality_checks

def run():
    logger = None
    try:
        config, logger, spark = init()
        logger.info("***    Quality layer execution started   ***")
        logger.info("***    Spark session created   ***")

        silver_path = config["paths"]["Silver"]
        Quality_path = config["paths"]["Quality"]
        reject_path_quality = config["paths"]["Rejected_data_quality"]

        run_quality_checks(spark, logger, silver_path, Quality_path, reject_path_quality)

        if spark:
            spark.stop()
            if logger:
                logger.info("******  spark session stopped  ******")
        logger.info("***    Quality layer execution ended   ***")

    except Exception as e:
        logger.error(f"Quality Layer Execution failed :{str(e)}")
        raise

if __name__ == "__main__":
    run()