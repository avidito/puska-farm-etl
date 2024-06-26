from datetime import date

from etl.helper.api.schemas import MLTriggerProduksi

from etl.seq_fact_produksi.modules.entity import (
    Produksi,
    FactProduksi,
)
from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
    FactProduksiMLRepository,
    FactProduksiWebSocketRepository,
)


class FactProduksiUsecase:
    __dwh_repo: FactProduksiDWHRepository
    __ml_repo: FactProduksiMLRepository
    __ws_repo: FactProduksiWebSocketRepository

    def __init__(
        self,
        dwh_repo: FactProduksiDWHRepository,
        ml_repo: FactProduksiMLRepository,
        ws_repo: FactProduksiWebSocketRepository,
    ):
        self.__dwh_repo = dwh_repo
        self.__ml_repo = ml_repo
        self.__ws_repo = ws_repo
    

    # Methods
    def get_or_create(
        self,
        tgl_produksi: date,
        id_unit_ternak: int,
        id_jenis_produk: int,
        sumber_pasokan: str,
    ) -> FactProduksi:
        fact_produksi_id = self.__dwh_repo.convert_id(tgl_produksi, id_unit_ternak, id_jenis_produk, sumber_pasokan)
        fact_produksi = self.__dwh_repo.get_or_create(fact_produksi_id)
        return fact_produksi


    def transform(self, new_produksi: Produksi, fact_produksi: FactProduksi) -> FactProduksi:
        fact_produksi.jumlah_produksi = fact_produksi.jumlah_produksi + new_produksi.jumlah
        return fact_produksi


    def load(self, fact_produksi: FactProduksi):
        self.__dwh_repo.load(fact_produksi)


    def predict_susu(self, id_waktu: int, id_lokasi: int, id_unit_peternakan: int):
        trigger = MLTriggerProduksi(
            id_waktu = id_waktu,
            id_lokasi = id_lokasi,
            id_unit_peternakan = id_unit_peternakan,
        )
        self.__ml_repo.predict_susu(trigger)
    
    def push_websocket(self):
        self.__ws_repo.push_susu()
        self.__ws_repo.push_ternak()
