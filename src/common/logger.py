import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

def get_logger(log_file="Logs/pipeline.log"):

    #create and return logger

    project_root = Path(__file__).resolve().parents[2]
    full_path = project_root / log_file

    # create logs folder if missing
    full_path.parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ecommerce_pipeline")

    if not logger.handlers:
        logger.setLevel(logging.INFO)

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(message)s"
        )

        file_handler = RotatingFileHandler(
            full_path,
            maxBytes=5 * 1024 * 1024,  # 5 MB
            backupCount=3
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
