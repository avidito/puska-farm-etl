from logging import Logger

from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusi,
    FactDistribusiID,
    KafkaDistribusi,
)
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
)


class FactDistribusiUsecase:
    __dwh_repo: FactDistribusiDWHRepository
    __logger: Logger

    def __init__(self, repo: FactDistribusiDWHRepository, logger: Logger):
        self.__dwh_repo = repo
        self.__logger = logger
    

    # Methods
    def load(self, kafka_distribusi: KafkaDistribusi, id_list: FactDistribusiID):
        exists_data = self.__dwh_repo.get(id_list)
        data = kafka_distribusi.data
        
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
        
        self.__logger.info(f"Loading data to 'Fact Distribusi'")
        self.__dwh_repo.load(distribusi)
