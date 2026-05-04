# empty object for unix features (error handle)
import socketserver
import os
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object
#-------------------------------------------------main code-------------------------------------------------------
from src.quality_layer.data_quality import run_quality_checks
from src.silver.customers import transform_customers
from src.silver.order_details import transform_order_details
from src.common.config import load_config
from src.common.spark import create_spark_session
from src.common.logger import get_logger
from src.silver.orders import transform_orders
from src.silver.payments import transform_payments
from src.silver.products import transform_products
from src.silver.reviews import transform_reviews
from src.silver.sellers import transform_sellers
from pyspark.sql.functions import *
from src.gold.fact_sales import build_fact_sales
from src.gold.customer_mart import customer_mart
from src.gold.product_performance_mart import product_performance_mart
from src.gold.sellers_performance import sellers_performance_mart

config = load_config()
spark = create_spark_session(config)
logger = get_logger()

bronze_path = config['paths']['Bronze']
silver_path = config["paths"]["Silver"]
Quality_path = config["paths"]["Quality"]
Gold_path = config["paths"]["Gold"]

df = spark.read.parquet(f'{silver_path}/orders')

df.show(20,truncate=False)

spark.stop()
