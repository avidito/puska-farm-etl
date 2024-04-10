import pytz
from logging import Logger
from typing import List
from datetime import datetime, timedelta

from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusi,
    FactDistribusiCalc,
    ParamsFactDistribusi,
)
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
    FactDistribusiOpsRepository,
)


class FactDistribusiUsecase:
    __dwh_repo: FactDistribusiDWHRepository
    __ops_repo: FactDistribusiOpsRepository
    __logger: Logger

    def __init__(self, dwh_repo: FactDistribusiDWHRepository, ops_repo: FactDistribusiOpsRepository, logger: Logger):
        self.__dwh_repo = dwh_repo
        self.__ops_repo = ops_repo
        self.__logger = logger
    

    # Methods
    def get(self, params: ParamsFactDistribusi) -> List[FactDistribusiCalc]:
        params.start_date = params.start_date if (params.start_date) else (datetime.now(pytz.timezone("Asia/Jakarta")) - timedelta(days=7)).strftime("%Y-%m-%d")
        params.end_date = params.end_date if (params.end_date) else datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d")

        self.__logger.info(f"Get 'Fact Distribusi' with params: {params.model_dump_json()}")
        fact_distribusi_calc = self.__ops_repo.get(params)
        return fact_distribusi_calc


    def load(self, fact_distribusi: List[FactDistribusi]) -> int:
        self.__logger.info("Load data to 'Fact Distribusi'")
        processed_row_count = self.__dwh_repo.load(fact_distribusi)
        return processed_row_count