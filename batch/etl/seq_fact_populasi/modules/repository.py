import os
from logging import Logger
from typing import List

from etl.helper.db import DWHHelper, OpsHelper
from etl.seq_fact_populasi.modules.entity import (
    FactPopulasi,
    FactPopulasiCalc,
    ParamsFactPopulasi,
)


class FactPopulasiDWHRepository:
    __dwh: DWHHelper
    __logger: Logger

    PK: List[str] = [
        "id_waktu",
        "id_lokasi",
        "id_peternakan",
        "jenis_kelamin",
        "tipe_ternak",
        "tipe_usia",
    ]

    def __init__(self, dwh: DWHHelper, logger: Logger):
        self.__dwh = dwh
        self.__logger = logger


    # Methods
    def load(self, populasi: List[FactPopulasi]) -> int:
        self.__logger.debug("Load data to 'Fact Populasi'")
        processed_row = self.__dwh.load(
            "fact_populasi",
            data = populasi,
            pk = self.PK,
            update_insert = True
        )
        processed_row = processed_row if (processed_row) else 0
        return processed_row
    

class FactPopulasiOpsRepository:
    __ops: OpsHelper
    __logger: Logger

    def __init__(self, ops: OpsHelper, logger: Logger):
        self.__ops = ops
        self.__logger = logger

        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")
    

    # Methods
    def get(self, params: ParamsFactPopulasi) -> List[FactPopulasiCalc]:
        prc_params = {
            "start_date": params.start_date,
            "end_date": params.end_date,
        }
        fact_populasi_calc = self.__calc_populasi(prc_params)
        return fact_populasi_calc


    # Private
    def __calc_populasi(self, prc_params: dict, query: str = "calculate_history_populasi.sql") -> List[FactPopulasiCalc]:
        self.__logger.debug(f"Run query '{query}'")
        results = self.__ops.run(self.__query_dir, query, prc_params)
        fact_populasi_calc = [FactPopulasiCalc(**row) for row in results]
        return fact_populasi_calc
