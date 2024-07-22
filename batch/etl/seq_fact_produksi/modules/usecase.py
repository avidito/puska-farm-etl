import pytz
from datetime import datetime
from typing import List

from etl.helper.log import LogBatch

from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
    FactProduksiOpsRepository,
)


class FactProduksiUsecase:
    __dwh_repo: FactProduksiDWHRepository
    __ops_repo: FactProduksiOpsRepository
    __log: LogBatch

    SOURCES: List[str] = [
        "produksi_susu",
        "produksi_ternak",
    ]

    def __init__(self, ops_repo: FactProduksiOpsRepository, dwh_repo: FactProduksiDWHRepository):
        self.__ops_repo = ops_repo
        self.__dwh_repo = dwh_repo
        self.__log = LogBatch(table_name = "fact_produksi")
    

    # Public
    def copy_source_cdc(self):
        for source in FactProduksiUsecase.SOURCES:
            self.__ops_repo.copy_cdc(source)
            self.__ops_repo.remove_cdc(source)


    def flag_cdc(self):
        for source in FactProduksiUsecase.SOURCES:
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


    def transform(self) -> int:
        fact_produksi_calc = self.__ops_repo.transform()
        processed_rows = self.__dwh_repo.update_dwh(fact_produksi_calc)
        return processed_rows
