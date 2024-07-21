import logging
from typing import Optional
from pydantic import BaseModel
from datetime import datetime


# Utility
def create_logger(levelname: str):
    log_level = {
        "error": logging.ERROR,
        "info": logging.INFO,
        "debug": logging.DEBUG,
    }[levelname.lower()]

    logger = logging.getLogger("sequence_logger")
    logger.setLevel(log_level)
    logger.propagate = False

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )

    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


# Entity
class LogBatch(BaseModel):
    table_name: str
    processed_rows: Optional[int] = None
    start_tm: Optional[datetime] = None
    end_tm: Optional[datetime] = None
    duration: Optional[float] = None
