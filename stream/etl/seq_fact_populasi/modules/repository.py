import os
from logging import Logger

from typing import List, Optional
from etl.helper.db import DWHHelper
from etl.seq_fact_populasi.modules.entity import (
    FactPopulasi,
    FactPopulasiID,
)


class FactPopulasiRepository:
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

        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def load(self, populasi: FactPopulasi):
        # self.__logger.info(populasi)
        self.__dwh.load(
            "fact_populasi_stream",
            data=[populasi],
            pk = self.PK,
            update_insert = True
        )
    
    def get(self, id_list: FactPopulasiID) -> Optional[FactPopulasi]:
        params = id_list.model_dump()
        results = self.__dwh.run(self.__query_dir, "get_fact_populasi.sql", params)
        
        fact_populasi = FactPopulasi(**results[0]) if (results) else None
        return fact_populasi
