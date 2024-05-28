from logging import Logger

from etl.seq_fact_produksi.modules.entity import (
    FactProduksi,
    FactProduksiID,
    KafkaProduksi,
)
from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
)


class FactProduksiUsecase:
    __dwh_repo: FactProduksiDWHRepository
    __logger: Logger

    def __init__(self, dwh_repo: FactProduksiDWHRepository, logger: Logger):
        self.__dwh_repo = dwh_repo
        self.__logger = logger
    

    # Methods
    def load(self, kafka_produksi: KafkaProduksi, id_list: FactProduksiID):
        exists_data = self.__dwh_repo.get(id_list)
        data = kafka_produksi.data
        
        produksi = FactProduksi(
            id_waktu = id_list.id_waktu,
            id_lokasi = id_list.id_lokasi,
            id_unit_peternak = id_list.id_unit_peternak,
            id_jenis_produk = id_list.id_jenis_produk,
            id_sumber_pasokan = id_list.id_sumber_pasokan,
            jumlah_produksi = (exists_data.jumlah_produksi + data.jumlah) if (exists_data) else data.jumlah,
        )
        
        self.__logger.info(f"Loading data to 'Fact Produksi'")
        self.__dwh_repo.load(produksi)
