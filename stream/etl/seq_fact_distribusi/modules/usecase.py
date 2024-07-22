from datetime import date
from typing import Optional

from etl.seq_fact_distribusi.modules.entity import (
    DistribusiSusu,
    DistribusiTernak,
    FactDistribusi,
)
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
    FactDistribusiWebSocketRepository,
)


class FactDistribusiUsecase:
    __dwh_repo: FactDistribusiDWHRepository
    __ws_repo: FactDistribusiWebSocketRepository

    def __init__(self, dwh_repo: FactDistribusiDWHRepository, ws_repo: FactDistribusiWebSocketRepository):
        self.__dwh_repo = dwh_repo
        self.__ws_repo = ws_repo
    

    # Methods
    def get_or_create(
        self,
        tgl_distribusi: date,
        id_unit_ternak: int,
        id_jenis_produk: int,
        id_mitra_bisnis: int,
    ) -> FactDistribusi:
        fact_distribusi_id = self.__dwh_repo.convert_id(tgl_distribusi, id_unit_ternak, id_jenis_produk, id_mitra_bisnis)
        fact_distribusi = self.__dwh_repo.get_or_create(fact_distribusi_id)
        return fact_distribusi
    

    def transform_susu(
        self,
        action: str,
        old_distribusi: Optional[DistribusiSusu],
        new_distribusi: Optional[DistribusiSusu],
        fact_distribusi: FactDistribusi
    ) -> FactDistribusi:
        if action == "INSERT":
            _jumlah_distribusi = fact_distribusi.jumlah_distribusi + new_distribusi.jumlah
            _harga_minimum = min(fact_distribusi.harga_minimum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_minimum != -1) else new_distribusi.harga_berlaku
            _harga_maximum = max(fact_distribusi.harga_maximum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_maximum != -1) else new_distribusi.harga_berlaku
            _harga_rata_rata = ((fact_distribusi.harga_rata_rata * fact_distribusi.jumlah_distribusi) + (new_distribusi.harga_berlaku * new_distribusi.jumlah)) / (fact_distribusi.jumlah_distribusi + new_distribusi.jumlah)
            _jumlah_penjualan = fact_distribusi.jumlah_penjualan + (new_distribusi.jumlah * new_distribusi.harga_berlaku)
        elif action == "UPDATE":
            _jumlah_distribusi = fact_distribusi.jumlah_distribusi - old_distribusi.jumlah + new_distribusi.jumlah
            _harga_minimum = min(fact_distribusi.harga_minimum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_minimum != -1) else new_distribusi.harga_berlaku
            _harga_maximum = max(fact_distribusi.harga_maximum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_maximum != -1) else new_distribusi.harga_berlaku
            _harga_rata_rata = ((fact_distribusi.harga_rata_rata * fact_distribusi.jumlah_distribusi) - (old_distribusi.harga_berlaku * old_distribusi.jumlah) + (new_distribusi.harga_berlaku * new_distribusi.jumlah)) / (fact_distribusi.jumlah_distribusi - old_distribusi.jumlah + new_distribusi.jumlah) if (fact_distribusi.jumlah_distribusi - old_distribusi.jumlah + new_distribusi.jumlah) else 0
            _jumlah_penjualan = fact_distribusi.jumlah_penjualan - (old_distribusi.jumlah * old_distribusi.harga_berlaku) + (new_distribusi.jumlah * new_distribusi.harga_berlaku)
        elif action == "DELETE":
            _jumlah_distribusi = fact_distribusi.jumlah_distribusi - old_distribusi.jumlah
            _harga_minimum = min(fact_distribusi.harga_minimum, old_distribusi.harga_berlaku) if (fact_distribusi.harga_minimum != -1) else old_distribusi.harga_berlaku
            _harga_maximum = max(fact_distribusi.harga_maximum, old_distribusi.harga_berlaku) if (fact_distribusi.harga_maximum != -1) else old_distribusi.harga_berlaku
            _harga_rata_rata = ((fact_distribusi.harga_rata_rata * fact_distribusi.jumlah_distribusi) - (old_distribusi.harga_berlaku * old_distribusi.jumlah)) / (fact_distribusi.jumlah_distribusi + old_distribusi.jumlah) if (fact_distribusi.jumlah_distribusi + old_distribusi.jumlah) else 0
            _jumlah_penjualan = fact_distribusi.jumlah_penjualan - (old_distribusi.jumlah * old_distribusi.harga_berlaku)
        
            if _harga_rata_rata == 0:
                    _harga_minimum = 0
                    _harga_maximum = 0
        
        fact_distribusi.jumlah_distribusi = _jumlah_distribusi
        fact_distribusi.jumlah_distribusi = _jumlah_distribusi
        fact_distribusi.harga_minimum = _harga_minimum
        fact_distribusi.harga_maximum = _harga_maximum
        fact_distribusi.harga_rata_rata = _harga_rata_rata
        fact_distribusi.jumlah_penjualan = _jumlah_penjualan
        return fact_distribusi


    def transform_ternak(
        self,
        action: str,
        old_distribusi: Optional[DistribusiTernak],
        new_distribusi: Optional[DistribusiTernak],
        fact_distribusi: FactDistribusi
    ) -> FactDistribusi:
        if action == "INSERT":
            _jumlah_distribusi = fact_distribusi.jumlah_distribusi + new_distribusi.jumlah
            _harga_minimum = min(fact_distribusi.harga_minimum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_minimum != -1) else new_distribusi.harga_berlaku
            _harga_maximum = max(fact_distribusi.harga_maximum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_maximum != -1) else new_distribusi.harga_berlaku
            _harga_rata_rata = ((fact_distribusi.harga_rata_rata * fact_distribusi.jumlah_distribusi) + (new_distribusi.harga_berlaku * new_distribusi.jumlah)) / (fact_distribusi.jumlah_distribusi + new_distribusi.jumlah)
            _jumlah_penjualan = fact_distribusi.jumlah_penjualan + (new_distribusi.jumlah * new_distribusi.harga_berlaku)
        elif action == "UPDATE":
            _jumlah_distribusi = fact_distribusi.jumlah_distribusi - old_distribusi.jumlah + new_distribusi.jumlah
            _harga_minimum = min(fact_distribusi.harga_minimum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_minimum != -1) else new_distribusi.harga_berlaku
            _harga_maximum = max(fact_distribusi.harga_maximum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_maximum != -1) else new_distribusi.harga_berlaku
            _harga_rata_rata = ((fact_distribusi.harga_rata_rata * fact_distribusi.jumlah_distribusi) - (old_distribusi.harga_berlaku * old_distribusi.jumlah) + (new_distribusi.harga_berlaku * new_distribusi.jumlah)) / (fact_distribusi.jumlah_distribusi - old_distribusi.jumlah + new_distribusi.jumlah) if (fact_distribusi.jumlah_distribusi - old_distribusi.jumlah + new_distribusi.jumlah) > 0 else 0
            _jumlah_penjualan = fact_distribusi.jumlah_penjualan - (old_distribusi.jumlah * old_distribusi.harga_berlaku) + (new_distribusi.jumlah * new_distribusi.harga_berlaku)
        elif action == "DELETE":
            _jumlah_distribusi = fact_distribusi.jumlah_distribusi - old_distribusi.jumlah
            _harga_minimum = min(fact_distribusi.harga_minimum, old_distribusi.harga_berlaku) if (fact_distribusi.harga_minimum != -1) else old_distribusi.harga_berlaku
            _harga_maximum = max(fact_distribusi.harga_maximum, old_distribusi.harga_berlaku) if (fact_distribusi.harga_maximum != -1) else old_distribusi.harga_berlaku
            _harga_rata_rata = ((fact_distribusi.harga_rata_rata * fact_distribusi.jumlah_distribusi) - (old_distribusi.harga_berlaku * old_distribusi.jumlah)) / (fact_distribusi.jumlah_distribusi - old_distribusi.jumlah) if (fact_distribusi.jumlah_distribusi - old_distribusi.jumlah) > 0 else 0
            _jumlah_penjualan = fact_distribusi.jumlah_penjualan - (old_distribusi.jumlah * old_distribusi.harga_berlaku)

            if _harga_rata_rata == 0:
                _harga_minimum = 0
                _harga_maximum = 0
        
        fact_distribusi.jumlah_distribusi = _jumlah_distribusi
        fact_distribusi.jumlah_distribusi = _jumlah_distribusi
        fact_distribusi.harga_minimum = _harga_minimum
        fact_distribusi.harga_maximum = _harga_maximum
        fact_distribusi.harga_rata_rata = _harga_rata_rata
        fact_distribusi.jumlah_penjualan = _jumlah_penjualan
        return fact_distribusi


    def load(self, fact_distribusi: FactDistribusi):
        self.__dwh_repo.load(fact_distribusi)


    def push_websocket(self):
        self.__ws_repo.push_susu()
        self.__ws_repo.push_ternak()
