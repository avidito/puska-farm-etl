import pytz
import logging
from typing import Optional
from pydantic import BaseModel
from datetime import datetime

from etl.helper.db import DWHHelper


# Utility
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
class LogBatch(BaseModel):
    table_name: str
    params: str
    processed_rows: Optional[int] = None
    start_tm: datetime
    end_tm: Optional[datetime] = None
    duration: Optional[float] = None


# Helper
class LogBatchHelper:
    __dwh: DWHHelper
    __entity: Optional[LogBatch]

    LOG_BATCH_TABLE: str = "log_batch"

    def __init__(self, dwh: DWHHelper):
        self.__dwh = dwh
        self.__entity = None
    

    # Methods
    def start_log(
        self,
        table_name: str,
        params: BaseModel,
    ):
        prc_params = params.model_dump_json()
        log_batch_en = LogBatch(
            table_name = table_name,
            params = prc_params,            
            start_tm = datetime.now(tz=pytz.timezone("Asia/Jakarta")),
        )

        self.__entity = log_batch_en


    def end_log(self, processed_rows: int):
        end_tm = datetime.now(tz=pytz.timezone("Asia/Jakarta"))
        duration = ((end_tm - self.__entity.start_tm).total_seconds()) * 1000 # Convert into milliseconds

        self.__entity.processed_rows = processed_rows
        self.__entity.end_tm = end_tm
        self.__entity.duration = duration

        self.__dwh.load(
            table = "log_batch",
            data = [self.__entity],
            pk = [],
            update_insert = False,
        )
