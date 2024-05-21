import os
from logging import Logger
from typing import List, Optional

from etl.helper.db import DWHHelper
from etl.helper.kafka import KafkaPushHelper
from etl.seq_fact_populasi.modules.entity import (
    FactPopulasi,
    FactPopulasiID,
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

        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def get(self, id_list: FactPopulasiID) -> Optional[FactPopulasi]:
        self.__logger.debug("Get data from 'Fact Poppulasi'")
        params = id_list.model_dump()
        results = self.__dwh.run(self.__query_dir, "get_fact_populasi.sql", params)
        
        fact_populasi = FactPopulasi(**results[0]) if (results) else None
        return fact_populasi


    def load(self, populasi: FactPopulasi):
        self.__logger.debug("Load data to 'Fact Populasi'")
        self.__dwh.load(
            "fact_populasi_stream",
            data=[populasi],
            pk = self.PK,
            update_insert = True
        )


class FactPopulasiKafkaRepository:
    __k_ternak: KafkaPushHelper
    __logger: Logger

    def __init__(self, k_ternak: KafkaPushHelper, logger: Logger):
        self.__k_ternak = k_ternak
        self.__logger = logger
    

    def push(self, fact_populasi: FactPopulasi):
        self.__logger.debug("Push data to Ternak")
        data = fact_populasi.model_dump_json()
        self.__k_ternak.push(data)
