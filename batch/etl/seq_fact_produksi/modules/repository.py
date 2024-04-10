import os
from logging import Logger
from typing import List

from etl.helper.db import OpsHelper, DWHHelper
from etl.seq_fact_produksi.modules.entity import (
    FactProduksi,
    FactProduksiCalc,
    ParamsFactProduksi,
)


class FactProduksiDWHRepository:
    __dwh: DWHHelper
    __logger: Logger

    PK: List[str] = [
        "id_waktu",
        "id_lokasi",
        "id_unit_peternak",
        "id_jenis_produk",
        "id_sumber_pasokan",
    ]

    def __init__(self, dwh: DWHHelper, logger: Logger):
        self.__dwh = dwh
        self.__logger = logger


    # Methods
    def load(self, produksi: List[FactProduksi]) -> int:
        self.__logger.debug("Load data to 'Fact Produksi'")
        processed_row = self.__dwh.load(
            "fact_produksi",
            data = produksi,
            pk = self.PK,
            update_insert = True
        )
        return processed_row
    

class FactProduksiOpsRepository:
    __ops: OpsHelper
    __logger: Logger

    def __init__(self, ops: OpsHelper, logger: Logger):
        self.__ops = ops
        self.__logger = logger

        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def get(self, params: ParamsFactProduksi) -> List[FactProduksi]:
        prc_params = {
            "start_date": params.start_date,
            "end_date": params.end_date,
        }
        fact_produksi_calc = self.__calc_produksi(prc_params)
        return fact_produksi_calc


    # Private
    def __calc_produksi(self, prc_params: dict, query: str = "calculate_produksi.sql") -> List[FactProduksi]:
        self.__logger.debug(f"Run query '{query}'")
        results = self.__ops.run(self.__query_dir, query, prc_params)
        fact_produksi_calc = [FactProduksiCalc(**row) for row in results]
        return fact_produksi_calc
