from logging import Logger

from etl.seq_fact_populasi.modules.entity import (
    FactPopulasi,
    FactPopulasiID,
    InputPopulasi,
)
from etl.seq_fact_populasi.modules.repository import FactPopulasiRepository


class FactPopulasiUsecase:
    __repo: FactPopulasiRepository
    __logger: Logger

    JENIS_KELAMIN = [
        "Jantan",
        "Betina",
    ]

    TIPE_TERNAK = [
        "Perah",
        "Pedaging",
    ]

    TIPE_USIA = [
        "Anakan",
        "Dewasa"
    ]

    def __init__(self, repo: FactPopulasiRepository, logger: Logger):
        self.__repo = repo
        self.__logger = logger
    

    # Methods
    def load(self, input_populasi: InputPopulasi, id_list: FactPopulasiID):
        data = input_populasi.data

        for jk in self.JENIS_KELAMIN:
            for tt in self.TIPE_TERNAK:
                for tu in self.TIPE_USIA:
                    value_column = "_".join(filter(None, [
                        "jml",
                        tt.lower(),
                        tu.lower() if tu == 'Anakan' else None,
                        jk.lower()
                    ]))

                    id_list_ext = id_list.model_copy(update={
                        "jenis_kelamin": jk,
                        "tipe_ternak": tt,
                        "tipe_usia": tu
                    })
                    exists_data = self.__repo.get(id_list_ext)
                    
                    populasi = FactPopulasi(
                        id_waktu = id_list_ext.id_waktu,
                        id_lokasi = id_list_ext.id_lokasi,
                        id_peternakan = id_list_ext.id_peternakan,
                        jenis_kelamin = id_list_ext.jenis_kelamin,
                        tipe_ternak = id_list_ext.tipe_ternak,
                        tipe_usia = id_list_ext.tipe_usia,
                        jumlah_lahir = exists_data.jumlah_lahir if (exists_data) else 0,
                        jumlah_mati = exists_data.jumlah_mati if (exists_data) else 0, 
                        jumlah_masuk = exists_data.jumlah_masuk if (exists_data) else 0, 
                        jumlah_keluar = exists_data.jumlah_keluar if (exists_data) else 0, 
                        jumlah = getattr(data, value_column, None)
                    )
                    # self.__logger.info(populasi)

                    self.__repo.load(populasi)
