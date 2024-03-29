from logging import Logger

from etl.seq_fact_produksi.modules.entity import (
    FactProduksi,
    FactProduksiID,
    InputProduksi,
)
from etl.seq_fact_produksi.modules.repository import FactProduksiRepository


class FactProduksiUsecase:
    __repo: FactProduksiRepository
    __logger: Logger

    def __init__(self, repo: FactProduksiRepository, logger: Logger):
        self.__repo = repo
        self.__logger = logger
    

    # Methods
    def load(self, input_produksi: InputProduksi, id_list: FactProduksiID):
        exists_data = self.__repo.get(id_list)
        data = input_produksi.data
        
        produksi = FactProduksi(
            id_waktu = id_list.id_waktu,
            id_lokasi = id_list.id_lokasi,
            id_unit_peternak = id_list.id_unit_peternak,
            id_jenis_produk = id_list.id_jenis_produk,
            id_sumber_pasokan = id_list.id_sumber_pasokan,
            jumlah_produksi = (exists_data.jumlah_produksi + data.jumlah) if (exists_data) else data.jumlah,
        )
        # self.__logger.info(produksi)

        self.__repo.load(produksi)
