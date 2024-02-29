import os
from typing import Optional, Tuple
from datetime import date
from sqlalchemy import create_engine, text
from sqlalchemy.exc import ResourceClosedError


# Public
SHARED_QUERY_DIR = os.path.join(os.path.dirname(__file__), "sql")

def get_dwh_ids(data: dict, convert_params: dict) -> dict:
    cvt_data = data
    for col, cvt in convert_params.items():
        cvt_data = __convert_id(cvt_data, col, cvt)
    return cvt_data


def get_dwh_ids_from_id_peternak(data: dict, columns: Tuple[str], pop: bool = True) -> dict:
    cvt_data = data
    for col in columns:
        result = run_query(
            f"get_{col}_from_id_peternak",
            SHARED_QUERY_DIR,
            {"id_peternak": data["id_peternak"]}
        )[0]
        cvt_data[col] = result[col]
    
    if (pop):
        cvt_data.pop("id_peternak")
    return cvt_data


def run_query(query_name: str, query_dir: str, params: Optional[dict] = None) -> list:
    params = params if (params) else {}

    query = __get_query(query_name, query_dir)
    db = __get_db()
    with db.connect() as conn:
        result_cursor = conn.execute(text(query), params)
        try:
            result_row = result_cursor.all()
        except ResourceClosedError:
            result_row = []
        finally:
            conn.commit()

    result = [row._mapping for row in result_row]
    return result


# Private - Converter
def __convert_id(data: dict, col: str, cvt: str) -> dict:
    def __tr_waktu(inp: date):
        # tahun, bulan, tanggal = inp.strftime("%Y-%m-%d").split("-")
        return {
            # "tahun": tahun,
            # "bulan": bulan,
            "tanggal": inp
        }
    
    try:
        # Get Query
        query_name, tr_params = {
            "id_sumber_pasokan": ("get_id_sumber_pasokan", lambda inp: {"sumber_pasokan": inp}),
            "id_waktu": ("get_id_waktu", __tr_waktu)
        }[cvt]
    except KeyError as key:
        raise KeyError(f"No ID Converter with types: {str(key)}.")
    
    try:
        # Get ID
        result = run_query(
            query_name,
            SHARED_QUERY_DIR,
            tr_params(data[col])
        )[0]
        
        # Replace Value
        data.pop(col)
        data[cvt] = result[cvt]
    except KeyError as key:
        raise KeyError(f"No column {str(key)} in the data.")
    
    return data


def __get_query(query_name: str, query_dir: str = SHARED_QUERY_DIR) -> str:
    query_path = os.path.join(query_dir, f"{query_name}.sql")
    with open(query_path, "r") as file:
        query = file.read()
    return query


# Private - Connection
from pydantic_settings import BaseSettings
from pydantic import Field, SecretStr

class ConfigDWHDatabase(BaseSettings):
    host: str = Field(alias="dwh_db_hostname")
    port: int = Field(alias="dwh_db_port")
    database: str = Field(alias="dwh_db_name")
    username: str = Field(alias="dwh_db_user")
    password: SecretStr = Field(alias="dwh_db_password")

CONFIG = ConfigDWHDatabase()

def __get_db():
    engine = create_engine(
        "postgresql://{username}:{password}@{host}:{port}/{database}".format(
            username = CONFIG.username,
            password = CONFIG.password.get_secret_value(),
            host = CONFIG.host,
            port = CONFIG.port,
            database = CONFIG.database
        ),
        future=True
    )
    return engine
