import os
from logging import Logger

from typing import List, Optional
from etl.helper.db import DWHHelper
from etl.seq_fact_produksi.modules.entity import (
    FactProduksi,
    FactProduksiID,
)


class FactProduksiRepository:
    __dwh: DWHHelper
    __logger: Logger

    PK: List[str] = [
        "id_waktu",
        "id_lokasi",
        "id_unit_peternak",
        "id_jenis_produk",
    ]

    def __init__(self, dwh: DWHHelper, logger: Logger):
        self.__dwh = dwh
        self.__logger = logger

        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def load(self, produksi: FactProduksi):
        # self.__logger.info(produksi)
        self.__dwh.load(
            "fact_produksi_stream",
            data=[produksi],
            pk = self.PK,
            update_insert = True
        )
    
    def get(self, id_list: FactProduksiID) -> Optional[FactProduksi]:
        params = id_list.model_dump()
        results = self.__dwh.run(self.__query_dir, "get_fact_produksi.sql", params)
        
        fact_produksi = FactProduksi(**results[0]) if (results) else None
        return fact_produksi
