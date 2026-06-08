# empty object for unix features (error handle)
import socketserver
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from src.common.bootstrap import init
from src.gold.transformation import gold_transformations

def run():
    logger = None
    try:
        config, logger, spark = init()
        logger.info("***    Gold layer execution started   ***")
        logger.info("***    Spark session created   ***")

        gold_transformations(spark, logger, config)

        if spark:
            spark.stop()
            if logger:
                logger.info("******  spark session stopped  ******")
        logger.info("***    Gold layer execution ended   ***")

    except Exception as e:
        logger.error(f"Gold Layer execution failed :{str(e)}")
        print(f"Gold Layer execution failed :{str(e)}")
        raise

if __name__ == "__main__":
    run()