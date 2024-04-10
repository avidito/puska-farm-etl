import pytz
from logging import Logger
from typing import List
from datetime import datetime, timedelta

from etl.seq_fact_produksi.modules.entity import (
    FactProduksi,
    FactProduksiCalc,
    ParamsFactProduksi,
)
from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
    FactProduksiOpsRepository,
)


class FactProduksiUsecase:
    __dwh_repo: FactProduksiDWHRepository
    __ops_repo: FactProduksiOpsRepository
    __logger: Logger

    def __init__(self, dwh_repo: FactProduksiDWHRepository, ops_repo: FactProduksiOpsRepository, logger: Logger):
        self.__dwh_repo = dwh_repo
        self.__ops_repo = ops_repo
        self.__logger = logger
    

    # Methods
    def get(self, params: ParamsFactProduksi) -> List[FactProduksiCalc]:
        params.start_date = params.start_date if (params.start_date) else (datetime.now(pytz.timezone("Asia/Jakarta")) - timedelta(days=7)).strftime("%Y-%m-%d")
        params.end_date = params.end_date if (params.end_date) else datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d")

        self.__logger.info(f"Get 'Fact Produksi' with params: {params.model_dump_json()}")
        fact_produksi_calc = self.__ops_repo.get(params)
        return fact_produksi_calc


    def load(self, fact_produksi: List[FactProduksi]) -> int:
        self.__logger.info("Load data to 'Fact Produksi'")
        processed_row_count = self.__dwh_repo.load(fact_produksi)
        return processed_row_count