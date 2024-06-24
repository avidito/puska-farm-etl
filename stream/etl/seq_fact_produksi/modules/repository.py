import os
from datetime import date
from logging import Logger
from typing import List, Literal, Optional

from etl.helper.api import MLHelper
from etl.helper.db import DWHHelper
from etl.helper.id_getter import IDGetterHelper

from etl.seq_fact_produksi.modules.entity import (
    FactProduksi,
    FactProduksiID,
)


class FactProduksiDWHRepository:
    __dwh: DWHHelper
    __logger: Logger
    __id_getter: IDGetterHelper
    __query_dir: str

    PK: List[str] = [
        "id_waktu",
        "id_lokasi",
        "id_unit_peternakan",
        "id_jenis_produk",
    ]

    def __init__(self, dwh: DWHHelper, logger: Logger):
        self.__dwh = dwh
        self.__logger = logger

        self.__id_getter = IDGetterHelper(dwh, logger)
        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def convert_id(
        self,
        tgl_produksi: date,
        id_unit_ternak: int,
        id_jenis_produk: int,
        sumber_pasokan: str,
    ) -> FactProduksiID:
        fact_produksi_id = FactProduksiID(
            id_waktu = self.__id_getter.get_id_waktu(tgl_produksi),
            id_lokasi = self.__id_getter.get_id_lokasi_from_unit_peternakan(id_unit_ternak),
            id_unit_peternakan = id_unit_ternak,
            id_jenis_produk = id_jenis_produk,
            id_sumber_pasokan = self.__id_getter.get_id_sumber_pasokan(sumber_pasokan),
        )
        return fact_produksi_id

    def get_or_create(self, fact_produksi_id: FactProduksiID) -> FactProduksi:
        self.__logger.debug("Get data from 'Fact Produksi'")
        results = self.__dwh.run(self.__query_dir, "get_fact_produksi.sql", fact_produksi_id.model_dump())
        
        if (results):
            fact_produksi = FactProduksi(**results[0])
        else:
            fact_produksi = FactProduksi(
                **fact_produksi_id.model_dump(),
                jumlah_produksi = 0,
            )
        return fact_produksi
    

    def load(self, produksi: FactProduksi):
        self.__logger.debug("Load data to 'Fact Produksi'")
        self.__dwh.load(
            "fact_produksi_stream",
            data = [produksi],
            pk = self.PK,
            update_insert = True
        )


class FactProduksiMLRepository:
    __ml: MLHelper
    __logger: Logger

    def __init__(self, ml: MLHelper, logger: Logger):
        self.__ml = ml
        self.__logger = logger
    
    def trigger_ml_susu(self, id_waktu: int, id_lokasi: int, id_unit_peternakan: int, time_type: Literal["daily", "weekly"] = "daily"):
        self.__logger.debug("Trigger ML Susu")
        self.__ml.trigger_ml_susu(time_type, id_waktu, id_lokasi, id_unit_peternakan)