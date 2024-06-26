import os
import json
from datetime import date
from logging import Logger
from typing import List, Optional

from etl.helper.db import DWHHelper
from etl.helper.id_getter import IDGetterHelper
from etl.helper.websocket import WebSocketHelper

from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusi,
    FactDistribusiID,
)


class FactDistribusiDWHRepository:
    __dwh: DWHHelper
    __logger: Logger
    __id_getter: IDGetterHelper
    __query_dir: str

    PK: List[str] = [
        "id_waktu",
        "id_lokasi",
        "id_unit_peternakan",
        "id_mitra_bisnis",
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
        tgl_distribusi: date,
        id_unit_ternak: int,
        id_jenis_produk: int,
        id_mitra_bisnis: int,
    ) -> FactDistribusiID:
        fact_distribusi_id = FactDistribusiID(
            id_waktu = self.__id_getter.get_id_waktu(tgl_distribusi),
            id_lokasi = self.__id_getter.get_id_lokasi_from_unit_peternakan(id_unit_ternak),
            id_unit_peternakan = id_unit_ternak,
            id_mitra_bisnis = id_mitra_bisnis,
            id_jenis_produk = id_jenis_produk,
        )
        return fact_distribusi_id

    def get_or_create(self, fact_distribusi_id: FactDistribusiID) -> Optional[FactDistribusi]:
        self.__logger.debug("Get data from 'Fact Distribusi'")
        results = self.__dwh.run(self.__query_dir, "get_fact_distribusi.sql", fact_distribusi_id.model_dump())
        
        if results:
            fact_distribusi = FactDistribusi(**results[0])
        else:
            fact_distribusi = FactDistribusi(
                **fact_distribusi_id.model_dump(),
                jumlah_distribusi = 0,
                harga_minimum = -1,
                harga_maximum = -1,
                harga_rata_rata = -1,
                jumlah_penjualan = 0,
            )
        return fact_distribusi


    def load(self, distribusi: FactDistribusi):
        self.__logger.debug("Load data to 'Fact Distribusi'")
        self.__dwh.load(
            "fact_distribusi_stream",
            data=[distribusi],
            pk = self.PK,
            update_insert = True
        )


class FactDistribusiWebSocketRepository:
    __ws: WebSocketHelper
    __logger: Logger

    def __init__(self, ws: WebSocketHelper, logger: Logger):
        self.__ws = ws
        self.__logger = logger
    
    def push_susu(self):
        self.__logger.debug("Push to WebSocket: etl-susu")
        payload = {
            "type": "etl-susu"
        }
        self.__ws.push(json.dumps(payload))
    
    def push_ternak(self):
        self.__logger.debug("Push to WebSocket: etl-ternak")
        payload = {
            "type": "etl-ternak"
        }
        self.__ws.push(json.dumps(payload))
