# empty object for unix features (error handle)
import socketserver
import os
if os.name == 'nt':
    socketserver.UnixStreamServer = object
    socketserver.UnixDatagramServer = object
#-------------------------------------------------main code-------------------------------------------------------

from src.common.config import load_config
from src.common.spark import create_spark_session
from src.common.logger import get_logger
from pyspark.sql.functions import *

config = load_config()
spark = create_spark_session(config)
logger = get_logger()

path = config['paths']['watermark']
s_path = config['paths']['screenshot']
df = spark.read.parquet(f"{path}")
df.coalesce(1).write.mode('overwrite').option("header", True).csv(f'{s_path}/watermark')

spark.stop()

