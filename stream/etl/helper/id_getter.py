import os
from typing import Optional, List
from datetime import date

from etl.helper.db import DWHHelper


class IDGetterHelper:
    __dwh: DWHHelper
    __query_dir: str

    def __init__(self, dwh: DWHHelper):
        self.__dwh = dwh
        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def get_id_waktu(self, tanggal: date) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_waktu.sql", {
            "tanggal": tanggal
        })
        
        id_waktu = results[0]["id"] if (results) else None
        return id_waktu

    
    def get_id_lokasi(self, provinsi: str, kabupaten_kota: str, kecamatan: str) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_lokasi.sql", {
            "provinsi": provinsi,
            "kabupaten_kota": kabupaten_kota,
            "kecamatan": kecamatan,
        })
        
        id_lokasi = results[0]["id"] if (results) else None
        return id_lokasi
    

    def get_id_lokasi_from_unit_ternak(self, id_unit_ternak: str) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_lokasi_from_unit_ternak.sql", {
            "id_unit_ternak": id_unit_ternak
        })
        
        if (results):
            params = {
                "provinsi": results[0]["provinsi"],
                "kabupaten_kota": results[0]["kabupaten_kota"],
                "kecamatan": results[0]["kecamatan"],
            }
            return self.get_id_lokasi(**params)
    

    def get_id_lokasi_from_peternakan(self, id_peternakan: str) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_lokasi_from_peternakan.sql", {
            "id_peternakan": id_peternakan
        })
        
        if (results):
            params = {
                "provinsi": results[0]["provinsi"],
                "kabupaten_kota": results[0]["kabupaten_kota"],
                "kecamatan": results[0]["kecamatan"],
            }
            return self.get_id_lokasi(**params)


    def get_id_sumber_pasokan(self, sumber_pasokan: str) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_sumber_pasokan.sql", {
            "nama_sumber_pasokan": sumber_pasokan
        })
        
        id_waktu = results[0]["id"] if (results) else None
        return id_waktu