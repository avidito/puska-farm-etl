from pydantic import BaseModel

import requests

from etl.helper.config import CONFIG


class MLUrl(BaseModel):
    ML_SUSU: str


class MLHelper:
    ml_url: MLUrl
    
    def __init__(self):
        ml_host = f"http://{CONFIG.API_ML_HOST}:{CONFIG.API_ML_PORT}"

        self.__ml_url = MLUrl(
            ML_SUSU = f"{ml_host}/predict"
        )

    def trigger_ml_susu(self, time_type: str, id_waktu: int, id_lokasi: int, id_unit_peternakan: str):
        req = requests.post(self.__ml_url.ML_SUSU, json={
            "time_type": time_type,
            "id_waktu": id_waktu,
            "id_lokasi": id_lokasi,
            "id_unit_peternakan": id_unit_peternakan,
        })
        req.raise_for_status()