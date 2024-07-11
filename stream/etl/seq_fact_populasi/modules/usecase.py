from typing import Dict
from datetime import date

from etl.seq_fact_populasi.modules.entity import (
    FactPopulasi,
    HistoryPopulasi,
    HistoryKelahiranKematian,
    PencatatanTernakMasuk,
    PencatatanTernakKeluar,
)
from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
    FactPopulasiWebSocketRepository,
)


class FactPopulasiUsecase:
    __dwh_repo: FactPopulasiDWHRepository
    __ws_repo: FactPopulasiWebSocketRepository

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

    def __init__(self, dwh_repo: FactPopulasiDWHRepository, ws_repo: FactPopulasiWebSocketRepository):
        self.__dwh_repo = dwh_repo
        self.__ws_repo = ws_repo
    

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
    
    def transform_kelahiran_kematian(self, new_lahir_mati: HistoryKelahiranKematian, fact_populasi: FactPopulasi) -> FactPopulasi:
        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if tipe_ternak == "Pedaging":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_lahir = new_lahir_mati.jml_lahir_pedaging_jantan
                    new_mati = new_lahir_mati.jml_mati_pedaging_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_lahir = 0
                    new_mati = new_lahir_mati.jml_mati_pedaging_jantan
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_lahir = new_lahir_mati.jml_lahir_pedaging_betina
                    new_mati = new_lahir_mati.jml_mati_pedaging_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_lahir = 0
                    new_mati = new_lahir_mati.jml_mati_pedaging_betina
        elif tipe_ternak == "Perah":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_lahir = new_lahir_mati.jml_lahir_perah_jantan
                    new_mati = new_lahir_mati.jml_mati_perah_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_lahir = 0
                    new_mati = new_lahir_mati.jml_mati_perah_jantan
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_lahir = new_lahir_mati.jml_lahir_perah_betina
                    new_mati = new_lahir_mati.jml_mati_perah_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_lahir = 0
                    new_mati = new_lahir_mati.jml_mati_perah_betina
        
        fact_populasi.jumlah_lahir = new_lahir
        fact_populasi.jumlah_mati = new_mati
    
    def transform_masuk(self, new_masuk: PencatatanTernakMasuk, fact_populasi: FactPopulasi) -> FactPopulasi:
        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if tipe_ternak == "Pedaging":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_masuk = new_masuk.jml_pedaging_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_masuk = new_masuk.jml_pedaging_jantan
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_masuk = new_masuk.jml_pedaging_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_masuk = new_masuk.jml_pedaging_betina
        elif tipe_ternak == "Perah":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_masuk = new_masuk.jml_perah_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_masuk = new_masuk.jml_perah_betina
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_masuk = new_masuk.jml_perah_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_masuk = new_masuk.jml_perah_betina
        
        fact_populasi.jumlah_masuk = new_masuk
    
    def transform_keluar(self, new_keluar: PencatatanTernakKeluar, fact_populasi: FactPopulasi) -> FactPopulasi:
        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if tipe_ternak == "Pedaging":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_keluar = new_keluar.jml_pedaging_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_keluar = new_keluar.jml_pedaging_jantan
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_keluar = new_keluar.jml_pedaging_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_keluar = new_keluar.jml_pedaging_betina
        elif tipe_ternak == "Perah":
            if jenis_kelamin == "Jantan":
                if tipe_usia == "Anakan":
                    new_keluar = new_keluar.jml_perah_anakan_jantan
                elif tipe_usia == "Dewasa":
                    new_keluar = new_keluar.jml_perah_betina
            elif jenis_kelamin == "Betina":
                if tipe_usia == "Anakan":
                    new_keluar = new_keluar.jml_perah_anakan_betina
                elif tipe_usia == "Dewasa":
                    new_keluar = new_keluar.jml_perah_betina
        
        fact_populasi.jumlah_keluar = new_keluar


    def load(self, fact_populasi: FactPopulasi):
        self.__dwh_repo.load(fact_populasi)

    def push_websocket(self):
        self.__ws_repo.push_ternak()
