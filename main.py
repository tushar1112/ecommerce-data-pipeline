# empty object for unix features (error handle)
import socketserver
import os

if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object
#-------------------------------------------------main code-------------------------------------------------------

from src.quality_layer.data_quality import run_quality_checks
from src.silver.transformations import process_file
from src.common.config import load_config
from src.common.logger import get_logger
from src.common.spark import create_spark_session
from src.bronze.ingestion import full_ingestion
from src.gold.customer_mart import customer_mart
from src.gold.fact_sales import build_fact_sales
from src.gold.product_performance_mart import product_performance_mart
from src.gold.sellers_performance import sellers_performance_mart

def main():

    spark = None
    logger = None

    try:
        config = load_config()
        logger = get_logger()
        logger.info("***    Pipeline execution started   ***")

        spark = create_spark_session(config)
        logger.info("   Spark session created successfully  ")

        raw_path = config["paths"]["raw"]
        bronze_path = config["paths"]["Bronze"]
        silver_path = config["paths"]["Silver"]
        reject_path_silver = config["paths"]["Rejected_data_silver"]
        Quality_path = config["paths"]["Quality"]
        reject_path_quality = config["paths"]["Rejected_data_quality"]
        gold_path = config["paths"]["Gold"]

        files = config["files"]
        silver_results = {}

        #bronze layer processing
        for file_name in files:
            dataset_name = file_name.replace('.csv','')
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
        logger.info('*****  Bronze layer processed successfully    *****')

        #silver layer processing
        for file_name in files:
            dataset_name = file_name.replace('.csv','')
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

        # Quality layer processing
        run_quality_checks(spark,logger,silver_path,Quality_path,reject_path_quality)

        #Gold layer processing
        build_fact_sales(spark,logger,Quality_path,gold_path)
        customer_mart(spark,logger,gold_path)
        product_performance_mart(spark,logger,Quality_path,gold_path)
        sellers_performance_mart(spark,logger,Quality_path,gold_path)

        logger.info(
            f"**********    Pipeline execution successfully completed   ***********"
        )

    except Exception as e:
        logger.error("*****     Pipeline execution failed    *****",{str(e)})
        raise
    finally:
        if spark:
            spark.stop()
            if logger:
                logger.info("******  spark session stopped  ******")

if __name__ == "__main__":
    main()