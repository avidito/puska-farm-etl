import os
import json
from datetime import date
from logging import Logger
from typing import List

from etl.helper.db import DWHHelper
from etl.helper.id_getter import IDGetterHelper
from etl.helper.websocket import WebSocketHelper

from etl.seq_fact_populasi.modules.entity import (
    FactPopulasi,
    FactPopulasiID,
)


class FactPopulasiDWHRepository:
    __dwh: DWHHelper
    __logger: Logger
    __id_getter: IDGetterHelper
    __query_dir: str

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

        self.__id_getter = IDGetterHelper(dwh, logger)
        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def convert_id(
        self,
        tgl_pencatatan: date,
        id_peternak: int
    ) -> FactPopulasiID:
        fact_populasi_id = FactPopulasiID(
            id_waktu = self.__id_getter.get_id_waktu(tgl_pencatatan),
            id_lokasi = self.__id_getter.get_id_lokasi_from_peternakan(id_peternak),
            id_peternakan = id_peternak,
        )
        return fact_populasi_id

    def get_or_create(self, fact_populasi_id: FactPopulasiID) -> FactPopulasi:
        self.__logger.debug("Get data from 'Fact Populasi'")
        results = self.__dwh.run(self.__query_dir, "get_fact_populasi.sql", fact_populasi_id.model_dump())

        if results:
            fact_populasi = FactPopulasi(**results[0])
        else:
            fact_populasi = FactPopulasi(
                **fact_populasi_id.model_dump(),
                jumlah_lahir = 0,
                jumlah_mati = 0,
                jumlah_masuk = 0,
                jumlah_keluar = 0,
                jumlah = 0,
            )
        return fact_populasi


    def load(self, populasi: FactPopulasi):
        self.__logger.debug("Load data to 'Fact Populasi'")
        self.__dwh.load(
            "fact_populasi_stream",
            data=[populasi],
            pk = self.PK,
            update_insert = True
        )


class FactPopulasiWebSocketRepository:
    __ws: WebSocketHelper
    __logger: Logger

    def __init__(self, ws: WebSocketHelper, logger: Logger):
        self.__ws = ws
        self.__logger = logger
    
    def push_ternak(self):
        self.__logger.debug("Push to WebSocket: etl-ternak")
        payload = {
            "type": "etl-ternak"
        }
        self.__ws.push(json.dumps(payload))
