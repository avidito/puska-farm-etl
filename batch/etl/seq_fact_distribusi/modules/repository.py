import os
from logging import Logger
from typing import List

from etl.helper.db import OpsHelper, DWHHelper
from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusi,
    FactDistribusiCalc,
    ParamsFactDistribusi,
)


class FactDistribusiDWHRepository:
    __dwh: DWHHelper
    __logger: Logger

    PK: List[str] = [
        "id_waktu",
        "id_lokasi",
        "id_unit_peternak",
        "id_mitra_bisnis",
        "id_jenis_produk",
    ]

    def __init__(self, dwh: DWHHelper, logger: Logger):
        self.__dwh = dwh
        self.__logger = logger


    # Methods
    def load(self, distribusi: List[FactDistribusi]) -> int:
        self.__logger.debug("Load data to 'Fact Distribusi'")
        processed_row = self.__dwh.load(
            "fact_distribusi",
            data = distribusi,
            pk = self.PK,
            update_insert = True
        )
        return processed_row
    

class FactDistribusiOpsRepository:
    __ops: OpsHelper
    __logger: Logger

    def __init__(self, ops: OpsHelper, logger: Logger):
        self.__ops = ops
        self.__logger = logger

        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def get(self, params: ParamsFactDistribusi) -> List[FactDistribusi]:
        prc_params = {
            "start_date": params.start_date,
            "end_date": params.end_date,
        }
        fact_distribusi_calc = self.__calc_distribusi(prc_params)
        return fact_distribusi_calc


    # Private
    def __calc_distribusi(self, prc_params: dict, query: str = "calculate_distribusi.sql") -> List[FactDistribusi]:
        self.__logger.debug(f"Run query '{query}'")
        results = self.__ops.run(self.__query_dir, query, prc_params)
        fact_distribusi_calc = [FactDistribusiCalc(**row) for row in results]
        return fact_distribusi_calc
