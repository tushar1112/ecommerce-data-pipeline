from src.common.config import load_config
from src.common.logger import get_logger
from src.common.spark import create_spark_session


def init():
    config = load_config()
    spark = create_spark_session(config)
    logger = get_logger()
    return  config,logger, spark