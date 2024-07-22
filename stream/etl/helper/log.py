import pytz
import logging
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from etl.helper.db import DWHHelper


def create_logger():
    logger = logging.getLogger("sequence_logger")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(message)s",
        datefmt = "%Y-%m-%d %H:%M:%S"
    )

    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


# Entity
class LogStream(BaseModel):
    table_name: str
    source_table: str
    action: str
    old_data: Optional[dict]
    new_data: Optional[dict]
    start_tm: datetime
    end_tm: Optional[datetime] = None
    duration: Optional[float] = None


# Helper
class LogStreamHelper:
    __dwh: DWHHelper
    __entity: Optional[LogStream]

    LOG_STREAM_TABLE: str = "log_stream"

    def __init__(self, dwh: DWHHelper):
        self.__dwh = dwh
        self.__entity = None
    

    # Methods
    def start_log(
        self,
        table_name: str,
        source_table: str,
        action: str,
        old_data: Optional[BaseModel],
        new_data: Optional[BaseModel],
    ):
        log_stream_en = LogStream(
            table_name = table_name,
            source_table = source_table,
            action = action,
            old_data = old_data.model_dump() if (old_data) else None,
            new_data = new_data.model_dump() if (new_data) else None,
            start_tm = datetime.now(tz=pytz.timezone("Asia/Jakarta")),
        )

        self.__entity = log_stream_en


    def end_log(self):
        end_tm = datetime.now(tz=pytz.timezone("Asia/Jakarta"))
        duration = ((end_tm - self.__entity.start_tm).total_seconds())

        self.__entity.end_tm = end_tm
        self.__entity.duration = duration

        self.__dwh.load(
            table = "log_stream",
            data = [self.__entity],
            pk = [],
            update_insert = False,
        )
