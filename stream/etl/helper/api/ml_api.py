from typing import Optional
from pydantic import BaseModel

import requests
from requests.exceptions import HTTPError

from etl.helper.config import CONFIG

from etl.helper.api.schemas import MLTriggerProduksi


class MLAPIPath(BaseModel):
    SusuPredict: str


class MLAPIHelper:
    ml_url: MLAPIPath
    
    def __init__(self):
        ml_api_host = f"http://{CONFIG.API_ML_HOST}:{CONFIG.API_ML_PORT}"

        self.__ml_url = MLAPIPath(
            SusuPredict = f"{ml_api_host}/predict"
        )

    def predict_susu(self, trigger: MLTriggerProduksi) -> Optional[str]:
        response = requests.post(
            self.__ml_url.SusuPredict,
            headers = {"Content-Type": "application/json"},
            data = trigger.model_dump_json()
        )

        try:
            response.raise_for_status()
            error = None
        except HTTPError:
            error = response.text
        finally:
            return error