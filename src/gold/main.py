# empty object for unix features (error handle)
import socketserver
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object

from src.common.bootstrap import init
from src.gold.customer_mart import customer_mart
from src.gold.fact_sales import build_fact_sales
from src.gold.product_performance_mart import product_performance_mart
from src.gold.sellers_performance import sellers_performance_mart


def run():
    logger = None
    try:
        config, logger, spark = init()
        logger.info("***    Gold layer execution started   ***")
        logger.info("***    Spark session created   ***")

        Quality_path = config["paths"]["Quality"]
        gold_path = config["paths"]["Gold"]

        build_fact_sales(spark, logger, Quality_path, gold_path)
        customer_mart(spark, logger, gold_path)
        product_performance_mart(spark, logger, Quality_path, gold_path)
        sellers_performance_mart(spark, logger, Quality_path, gold_path)

        if spark:
            spark.stop()
            if logger:
                logger.info("******  spark session stopped  ******")
        logger.info("***    Gold layer execution ended   ***")

    except Exception as e:
        logger.error(f"Gold Layer execution failed :{str(e)}")
        raise

if __name__ == "__main__":
    run()