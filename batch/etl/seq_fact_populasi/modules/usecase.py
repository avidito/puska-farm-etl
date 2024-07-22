import pytz
from datetime import datetime
from typing import List

from etl.helper.log import LogBatch

from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
    FactPopulasiOpsRepository,
)


class FactPopulasiUsecase:
    __dwh_repo: FactPopulasiDWHRepository
    __ops_repo: FactPopulasiOpsRepository

    SOURCES: List[str] = [
        "history_populasi",
        "history_kelahiran_kematian",
        "pencatatan_ternak_masuk",
        "pencatatan_ternak_keluar",
    ]

    def __init__(self, ops_repo: FactPopulasiOpsRepository, dwh_repo: FactPopulasiDWHRepository):
        self.__ops_repo = ops_repo
        self.__dwh_repo = dwh_repo
        self.__log = LogBatch(table_name = "fact_populasi")
    

    # Public
    def copy_source_cdc(self):
        for source in FactPopulasiUsecase.SOURCES:
            self.__ops_repo.copy_cdc(source)
            self.__ops_repo.remove_cdc(source)


    def flag_cdc(self):
        for source in FactPopulasiUsecase.SOURCES:
            self.__ops_repo.flag_cdc(source)
    
    
    def log_end(self, processed_rows: int) -> float:
        end_tm = datetime.now(pytz.timezone("Asia/Jakarta"))
        self.__log.end_tm = end_tm

        duration = (end_tm - self.__log.start_tm).total_seconds()
        self.__log.duration = duration

        self.__log.processed_rows = processed_rows

        self.__dwh_repo.load_log(self.__log)
        return duration


    def log_start(self):
        start_tm = datetime.now(pytz.timezone("Asia/Jakarta"))
        self.__log.start_tm = start_tm


    def transform(self) -> float:
        fact_distribusi_calc = self.__ops_repo.transform()
        processed_rows = self.__dwh_repo.update_dwh(fact_distribusi_calc)
        return processed_rows
