from datetime import date

from etl.seq_fact_distribusi.modules.entity import (
    Distribusi,
    FactDistribusi,
)
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
)


class FactDistribusiUsecase:
    __dwh_repo: FactDistribusiDWHRepository

    def __init__(self, dwh_repo: FactDistribusiDWHRepository):
        self.__dwh_repo = dwh_repo
    

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
    

    def transform(self, new_distribusi: Distribusi, fact_distribusi: FactDistribusi) -> FactDistribusi:
        fact_distribusi.jumlah_distribusi = fact_distribusi.jumlah_distribusi + new_distribusi.jumlah
        fact_distribusi.harga_minimum = min(fact_distribusi.harga_minimum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_minimum != -1) else new_distribusi.harga_berlaku
        fact_distribusi.harga_maximum = max(fact_distribusi.harga_maximum, new_distribusi.harga_berlaku) if (fact_distribusi.harga_maximum != -1) else new_distribusi.harga_berlaku
        fact_distribusi.harga_rata_rata = (fact_distribusi.jumlah_penjualan + (new_distribusi.jumlah * new_distribusi.harga_berlaku)) / (fact_distribusi.jumlah_distribusi + new_distribusi.jumlah) if (fact_distribusi.harga_rata_rata != -1) else new_distribusi.harga_berlaku
        fact_distribusi.jumlah_penjualan = fact_distribusi.jumlah_penjualan + (new_distribusi.jumlah * new_distribusi.harga_berlaku)
        return fact_distribusi


    def load(self, fact_distribusi: FactDistribusi):
        self.__dwh_repo.load(fact_distribusi)
