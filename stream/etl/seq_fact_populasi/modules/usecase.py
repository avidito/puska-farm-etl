from typing import Dict
from datetime import date

from etl.seq_fact_populasi.modules.entity import (
    HistoryPopulasi,
    FactPopulasi,
    FactPopulasiID,
)
from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
)


class FactPopulasiUsecase:
    __dwh_repo: FactPopulasiDWHRepository

    TIPE_TERNAK = [
        "Pedaging",
        "Perah",
    ]

    JENIS_KELAMIN = [
        "Jantan",
        "Betina",
    ]

    TIPE_USIA = [
        "Anakan",
        "Dewasa",
    ]

    def __init__(self, dwh_repo: FactPopulasiDWHRepository):
        self.__dwh_repo = dwh_repo
    

    # Methods
    def get_or_create(
        self,
        tgl_pencatatan: date,
        id_peternak: int,
    ) -> Dict[str, FactPopulasi]:
        fact_populasi_base_id = self.__dwh_repo.convert_id(tgl_pencatatan, id_peternak)
        fact_populasi_id = {
            "pedaging_jantan_anakan": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Pedaging", "jenis_kelamin":"Jantan", "tipe_usia":"Anakan"}),
            "pedaging_jantan_dewasa": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Pedaging", "jenis_kelamin":"Jantan", "tipe_usia":"Dewasa"}),
            "pedaging_betina_anakan": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Pedaging", "jenis_kelamin":"Betina", "tipe_usia":"Anakan"}),
            "pedaging_betina_dewasa": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Pedaging", "jenis_kelamin":"Betina", "tipe_usia":"Dewasa"}),
            "perah_jantan_anakan": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Perah", "jenis_kelamin":"Jantan", "tipe_usia":"Anakan"}),
            "perah_jantan_dewasa": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Perah", "jenis_kelamin":"Jantan", "tipe_usia":"Dewasa"}),
            "perah_betina_anakan": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Perah", "jenis_kelamin":"Betina", "tipe_usia":"Anakan"}),
            "perah_betina_dewasa": fact_populasi_base_id.model_copy(update={"tipe_ternak":"Perah", "jenis_kelamin":"Betina", "tipe_usia":"Dewasa"}),
        }

        fact_populasi = {
            k: self.__dwh_repo.get_or_create(id)
            for k, id in fact_populasi_id.items()
        }
        return fact_populasi

    def transform_jumlah(self, new_populasi: HistoryPopulasi, fact_populasi: FactPopulasi) -> FactPopulasi:
        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if tipe_ternak == "Pedaging":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_jumlah = new_populasi.jml_pedaging_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_jumlah = new_populasi.jml_pedaging_jantan
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_jumlah = new_populasi.jml_pedaging_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_jumlah = new_populasi.jml_pedaging_betina
        elif tipe_ternak == "Perah":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_jumlah = new_populasi.jml_pedaging_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_jumlah = new_populasi.jml_pedaging_jantan
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_jumlah = new_populasi.jml_pedaging_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_jumlah = new_populasi.jml_pedaging_betina
        
        fact_populasi.jumlah = new_jumlah
        return fact_populasi


    def load(self, fact_populasi: FactPopulasi):
        self.__dwh_repo.load(fact_populasi)
