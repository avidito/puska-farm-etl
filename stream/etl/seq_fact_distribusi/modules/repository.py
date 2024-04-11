import os
from logging import Logger
from typing import List, Optional

from etl.helper.db import DWHHelper
from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusi,
    FactDistribusiID,
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

        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def get(self, id_list: FactDistribusiID) -> Optional[FactDistribusi]:
        self.__logger.debug("Get data from 'Fact Distribusi'")
        params = id_list.model_dump()
        results = self.__dwh.run(self.__query_dir, "get_fact_distribusi.sql", params)
        
        fact_distribusi = FactDistribusi(**results[0]) if (results) else None
        return fact_distribusi


    def load(self, distribusi: FactDistribusi):
        self.__logger.debug("Load data to 'Fact Distribusi'")
        self.__dwh.load(
            "fact_distribusi_stream",
            data=[distribusi],
            pk = self.PK,
            update_insert = True
        )
