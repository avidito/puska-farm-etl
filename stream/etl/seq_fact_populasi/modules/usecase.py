from typing import Dict, Optional
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


    def transform_jumlah(
        self,
        action: str,
        old_populasi: Optional[HistoryPopulasi],
        new_populasi: Optional[HistoryPopulasi],
        fact_populasi: FactPopulasi
    ) -> FactPopulasi:
        _ = old_populasi # Unused

        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if (action == "INSERT" or action == "UPDATE"):
            if tipe_ternak == "Pedaging":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _jumlah = new_populasi.jml_pedaging_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _jumlah = new_populasi.jml_pedaging_jantan
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _jumlah = new_populasi.jml_pedaging_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _jumlah = new_populasi.jml_pedaging_betina
            elif tipe_ternak == "Perah":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _jumlah = new_populasi.jml_pedaging_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _jumlah = new_populasi.jml_pedaging_jantan
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _jumlah = new_populasi.jml_pedaging_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _jumlah = new_populasi.jml_pedaging_betina
            
            fact_populasi.jumlah = _jumlah

        elif action == "DELETE":
            fact_populasi.jumlah = 0

        return fact_populasi


    def transform_kelahiran_kematian(
        self,
        action: str,
        old_lahir_mati: Optional[HistoryKelahiranKematian],
        new_lahir_mati: Optional[HistoryKelahiranKematian],
        fact_populasi: FactPopulasi
    ) -> FactPopulasi:
        _ = old_lahir_mati # Unused

        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if (action == "INSERT") or (action == "UPDATE"):
            if tipe_ternak == "Pedaging":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _lahir = new_lahir_mati.jml_lahir_pedaging_jantan
                        _mati = new_lahir_mati.jml_mati_pedaging_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _lahir = 0
                        _mati = new_lahir_mati.jml_mati_pedaging_jantan
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _lahir = new_lahir_mati.jml_lahir_pedaging_betina
                        _mati = new_lahir_mati.jml_mati_pedaging_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _lahir = 0
                        _mati = new_lahir_mati.jml_mati_pedaging_betina
            elif tipe_ternak == "Perah":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _lahir = new_lahir_mati.jml_lahir_perah_jantan
                        _mati = new_lahir_mati.jml_mati_perah_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _lahir = 0
                        _mati = new_lahir_mati.jml_mati_perah_jantan
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _lahir = new_lahir_mati.jml_lahir_perah_betina
                        _mati = new_lahir_mati.jml_mati_perah_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _lahir = 0
                        _mati = new_lahir_mati.jml_mati_perah_betina
                
            fact_populasi.jumlah_lahir = _lahir
            fact_populasi.jumlah_mati = _mati

        elif action == "DELETE":
            fact_populasi.jumlah_lahir = 0
            fact_populasi.jumlah_mati = 0

        return fact_populasi


    def transform_masuk(
        self,
        action: str,
        old_masuk: Optional[PencatatanTernakMasuk],
        new_masuk: Optional[PencatatanTernakMasuk],
        fact_populasi: FactPopulasi
    ) -> FactPopulasi:
        _ = old_masuk #Unused

        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if (action == "INSERT") or (action == "UPDATE"):
            if tipe_ternak == "Pedaging":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _masuk = new_masuk.jml_pedaging_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _masuk = new_masuk.jml_pedaging_jantan
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _masuk = new_masuk.jml_pedaging_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _masuk = new_masuk.jml_pedaging_betina
            elif tipe_ternak == "Perah":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _masuk = new_masuk.jml_perah_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _masuk = new_masuk.jml_perah_betina
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _masuk = new_masuk.jml_perah_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _masuk = new_masuk.jml_perah_betina
            
            fact_populasi.jumlah_masuk = _masuk
        
        elif action == "DELETE":
            fact_populasi.jumlah_masuk = 0

        return fact_populasi


    def transform_keluar(
        self,
        action: str,
        old_keluar: Optional[PencatatanTernakKeluar],
        new_keluar: Optional[PencatatanTernakKeluar],
        fact_populasi: FactPopulasi
    ) -> FactPopulasi:
        _ = old_keluar # Unused

        tipe_ternak = fact_populasi.tipe_ternak
        jenis_kelamin = fact_populasi.jenis_kelamin
        tipe_usia = fact_populasi.tipe_usia

        if (action == "INSERT") or (action == "UPDATE"):
            if tipe_ternak == "Pedaging":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _keluar = new_keluar.jml_pedaging_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _keluar = new_keluar.jml_pedaging_jantan
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _keluar = new_keluar.jml_pedaging_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _keluar = new_keluar.jml_pedaging_betina
            elif tipe_ternak == "Perah":
                if jenis_kelamin == "Jantan":
                    if tipe_usia == "Anakan":
                        _keluar = new_keluar.jml_perah_anakan_jantan
                    elif tipe_usia == "Dewasa":
                        _keluar = new_keluar.jml_perah_betina
                elif jenis_kelamin == "Betina":
                    if tipe_usia == "Anakan":
                        _keluar = new_keluar.jml_perah_anakan_betina
                    elif tipe_usia == "Dewasa":
                        _keluar = new_keluar.jml_perah_betina

            fact_populasi.jumlah_keluar = _keluar

        elif action == "DELETE":
            fact_populasi.jumlah_keluar = 0
        return fact_populasi


    def load(self, fact_populasi: FactPopulasi):
        self.__dwh_repo.load(fact_populasi)


    def push_websocket(self):
        self.__ws_repo.push_ternak()
