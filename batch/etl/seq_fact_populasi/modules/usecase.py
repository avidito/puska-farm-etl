import pytz
from logging import Logger
from typing import List
from datetime import datetime, timedelta

from etl.seq_fact_populasi.modules.entity import (
    FactPopulasi,
    FactPopulasiCalc,
    ParamsFactPopulasi,
)
from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
    FactPopulasiOpsRepository,
)


class FactPopulasiUsecase:
    __dwh_repo: FactPopulasiDWHRepository
    __ops_repo: FactPopulasiOpsRepository
    __logger: Logger

    def __init__(self, dwh_repo: FactPopulasiDWHRepository, ops_repo: FactPopulasiOpsRepository, logger: Logger):
        self.__dwh_repo = dwh_repo
        self.__ops_repo = ops_repo
        self.__logger = logger
    

    # Methods
    def get(self, params: ParamsFactPopulasi) -> List[FactPopulasiCalc]:
        params.start_date = params.start_date if (params.start_date) else (datetime.now(pytz.timezone("Asia/Jakarta")) - timedelta(days=7)).strftime("%Y-%m-%d")
        params.end_date = params.end_date if (params.end_date) else datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d")

        self.__logger.info(f"Get 'Fact Populasi' with params: {params.model_dump_json()}")
        fact_populasi_calc = self.__ops_repo.get(params)
        return fact_populasi_calc
    

    def load(self, fact_populasi: List[FactPopulasi]) -> int:
        self.__logger.info("Load data to 'Fact Populasi'")
        processed_row_count = self.__dwh_repo.load(fact_populasi)
        return processed_row_count
