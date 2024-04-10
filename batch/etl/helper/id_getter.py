import os
from logging import Logger
from typing import Optional
from datetime import date

from etl.helper.db import DWHHelper


class IDGetterHelper:
    __dwh: DWHHelper
    __logger: Logger
    __query_dir: str

    def __init__(self, dwh: DWHHelper, logger: Logger):
        self.__dwh = dwh
        self.__logger = logger
        self.__query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Methods
    def get_id_waktu(self, tanggal: date) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_waktu.sql", {
            "tanggal": tanggal
        })
        
        if (results):
            id_waktu = results[0]["id"]
            return id_waktu
        else:
            self.__logger.error(f"No waktu for 'tanggal' = '{tanggal}'")

    
    def get_id_lokasi(self, provinsi: str, kabupaten_kota: str, kecamatan: str) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_lokasi.sql", {
            "provinsi": provinsi,
            "kabupaten_kota": kabupaten_kota,
            "kecamatan": kecamatan,
        })
        
        if (results):
            id_lokasi = results[0]["id"]
            return id_lokasi
        else:
            self.__logger.error(f"No lokasi for 'provinsi' = '{provinsi}', 'kabupaten_kota' = '{kabupaten_kota}', 'kecamatan' = '{kecamatan}'")
    

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
        else:
            self.__logger.error(f"No unit_ternak for 'id_unit_ternak' = {id_unit_ternak}")
    

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
        else:
            self.__logger.error(f"No peternakan for 'id_peternakan' = '{id_peternakan}'")


    def get_id_sumber_pasokan(self, sumber_pasokan: str) -> Optional[int]:
        results = self.__dwh.run(self.__query_dir, "get_id_sumber_pasokan.sql", {
            "nama_sumber_pasokan": sumber_pasokan
        })
        
        if (results):
            id_waktu = results[0]["id"]
            return id_waktu
        else:
            self.__logger.error(f"No sumber_pasokan for 'nama_sumber_pasokan' = '{sumber_pasokan}'")