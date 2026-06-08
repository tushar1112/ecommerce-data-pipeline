# empty object for unix features (error handle)
import socketserver
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from src.common.bootstrap import init
from src.silver.transformations import silver_full_processing, silver_incremental_processing


def run():

    logger = None

    try:
        config, logger, spark = init()
        logger.info("***    Silver layer execution started   ***")
        logger.info("***    Spark session created   ***")
        load_type = config["load_type"]

        if load_type == "FULL_LOAD":
            silver_full_processing(spark,logger,config)
        elif load_type == "INC":
            silver_incremental_processing(spark,logger,config)
        else:
            raise ValueError(f"Invalid load_type: {load_type}")

        # ending sparks session
        if spark:
            spark.stop()
            if logger:
                logger.info("***    spark session stopped   ***")

        #closing silver processing
        logger.info("***    Silver layer Processing ended   ***")

    except Exception as e:
        logger.error(f" Silver Layer Processing failed : {str(e)}")
        raise

if __name__ == "__main__":
    run()