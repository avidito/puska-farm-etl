from logging import Logger

from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusi,
    FactDistribusiID,
    InputDistribusi,
)
from etl.seq_fact_distribusi.modules.repository import FactDistribusiRepository


class FactDistribusiUsecase:
    __repo: FactDistribusiRepository
    __logger: Logger

    def __init__(self, repo: FactDistribusiRepository, logger: Logger):
        self.__repo = repo
        self.__logger = logger
    

    # Methods
    def load(self, input_distribusi: InputDistribusi, id_list: FactDistribusiID):
        exists_data = self.__repo.get(id_list)
        data = input_distribusi.data
        
        distribusi = FactDistribusi(
            id_waktu = id_list.id_waktu,
            id_lokasi = id_list.id_lokasi,
            id_unit_peternak = id_list.id_unit_peternak,
            id_mitra_bisnis = id_list.id_mitra_bisnis,
            id_jenis_produk = id_list.id_jenis_produk,
            jumlah_distribusi = (exists_data.jumlah_distribusi + data.jumlah) if (exists_data) else data.jumlah,
            harga_minimum = min(exists_data.harga_minimum, data.harga_berlaku) if (exists_data) else data.harga_berlaku,
            harga_maximum = max(exists_data.harga_maximum, data.harga_berlaku) if (exists_data) else data.harga_berlaku,
            harga_rata_rata = (exists_data.jumlah_penjualan + (data.jumlah * data.harga_berlaku)) / (exists_data.jumlah_distribusi + data.jumlah) if (exists_data) else data.harga_berlaku,
            jumlah_penjualan = exists_data.jumlah_penjualan + (data.jumlah * data.harga_berlaku) if (exists_data) else (data.jumlah * data.harga_berlaku),
        )
        # self.__logger.info(distribusi)

        self.__repo.load(distribusi)
