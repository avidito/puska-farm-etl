import os
import json
from typing import Callable, Dict, List, Optional
from datetime import date
from pydantic import BaseModel

from sqlalchemy import Engine, Connection, TextClause, create_engine, text
from urllib.parse import quote_plus

from etl.helper.config import CONFIG


# Getter
def get_dwh() -> Engine:
    engine = create_engine(
        "{dialect}://{user}:{password}@{host}:{port}/{db}".format(
            dialect = CONFIG.DWH_DIALECT,
            user = CONFIG.DWH_USER,
            password = quote_plus(CONFIG.DWH_PASSWORD.get_secret_value()),
            host = CONFIG.DWH_HOST,
            port = CONFIG.DWH_PORT,
            db = CONFIG.DWH_DB,
        )
    )
    return engine

def get_ops() -> Engine:
    engine = create_engine(
        "{dialect}://{user}:{password}@{host}:{port}/{db}".format(
            dialect = CONFIG.OPS_DIALECT,
            user = CONFIG.OPS_USER,
            password = quote_plus(CONFIG.OPS_PASSWORD.get_secret_value()),
            host = CONFIG.OPS_HOST,
            port = CONFIG.OPS_PORT,
            db = CONFIG.OPS_DB,
        )
    )
    return engine

DB_GENERATOR: Dict[str, Callable] = {
    "DWH": get_dwh,
    "OPS": get_ops,
}


# Helper
class PostgreSQLHelper:
    __db: Engine
    __helper_query_dir: str

    def __init__(self, mode: str):
        try:
            self.__db = DB_GENERATOR[mode]()
        except KeyError:
            raise KeyError(f"No DB_GENERATOR for mode: '{mode}'")
        
        self.__helper_query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Public - CDC
    def copy_cdc(self, table_name: str):
        self.__run_query(self.__helper_query_dir, "copy_cdc", {"table_name": table_name}, with_result = False)


    def remove_cdc(self, table_name: str):
        self.__run_query(self.__helper_query_dir, "remove_cdc", {"table_name": table_name}, with_result = False)


    def flag_cdc(self, table_name: str):
        self.__run_query(self.__helper_query_dir, "flag_cdc", {"table_name": table_name}, with_result = False)


    # Public - SQL Getter
    def load(self, table_name: str, data: List[BaseModel]) -> int:
        with self.__db.connect() as conn:
            if data:
                    data_dict = [row.model_dump() for row in data]
                    columns = data_dict[0].keys()

                    load_query = self.__generate_load_query(table_name, columns, data_dict)
                    result_it = conn.execute(load_query, {})
                    result = [dict(row) for row in result_it.mappings().all()]
                    processed_rows = result[0]
            else:
                processed_rows = 0
            
            conn.commit()
        return processed_rows


    def load_session(self, conn: Connection, table_name: str, data: List[BaseModel]) -> int:
        if data:
            data_dict = [row.model_dump() for row in data]
            columns = data_dict[0].keys()

            load_query = self.__generate_load_query(table_name, columns, data_dict)
            result_it = conn.execute(load_query, {})
            result = [dict(row) for row in result_it.mappings().all()]
            processed_rows = result[0]["processed_rows"]
            return processed_rows
        else:
            return 0


    def get_data(self, query_dir: str, query_name: str, params: Optional[dict] = None) -> List[dict]:
        result = self.__run_query(query_dir, query_name, params)
        return result


    def get_db(self) -> Engine:
        return self.__db


    def run_query_session(self, conn: Connection, query_dir: str, query_name: str, params: Optional[None] = {}):
        query_file = os.path.join(query_dir, f"{query_name}.sql")
        with open(query_file, "r") as file:
            query = text(file.read())

        conn.execute(query, params)


    # Private
    def __generate_load_query(
        self,
        table: str,
        columns: List[str],
        data: List[dict],
    ) -> TextClause:
        # Define Insert Statement
        insert_statement = "\n".join([
            f"  INSERT INTO {table} (",
            *[
                f"    {c},"
                for c in columns
            ],
            "    created_dt,",
            "    modified_dt",
            "  )",
            "  VALUES ",
        ])

        # Define Insert Values Statement
        insert_value_rows = []
        for row in data:
            insert_value = []
            for col in columns:
                if (row[col] == "" or row[col] is None):
                    insert_value.append("NULL")
                elif (
                    isinstance(row[col], int)
                    or isinstance(row[col], float)
                ):
                    insert_value.append(str(row[col]))
                elif (
                    isinstance(row[col], bytes)
                ):
                    insert_value.append(f"b'{int.from_bytes(row[col], 'big')}'")
                elif (
                    isinstance(row[col], str)
                    or isinstance(row[col], date)
                ):
                    insert_value.append(f"'{row[col]}'".replace(":", r"\:"))
                elif (
                    isinstance(row[col], dict)
                ):
                    insert_value.append(f"'{json.dumps(row[col])}'".replace(":", r"\:"))
            
            insert_value += [
                "TIMEZONE('Asia/Jakarta', NOW())",
                "TIMEZONE('Asia/Jakarta', NOW())"
            ]

            insert_value_rows.append(f"({','.join(insert_value)})")
        
        insert_value_statement = ", ".join(insert_value_rows)

        query = insert_statement + insert_value_statement
        
        # Get Row Count
        query = "\n".join([
            "WITH data_load AS (",
            query,
            "  RETURNING 1",
            ")",
            "SELECT COUNT(0) AS processed_rows FROM data_load;",
        ])
        return text(query)

    def __get_query(self, query_dir: str, query_name: str) -> TextClause:
        query_path = os.path.join(query_dir, f"{query_name}.sql")
        with open(query_path, "r") as file:
            query = text(file.read())
        return query


    def __run_query(self, query_dir: str, query_name: str, params: Optional[dict] = None, with_result: bool = True) -> List[dict]:
        query = self.__get_query(query_dir, query_name)
        with self.__db.connect() as conn:
            result_it = conn.execute(query, parameters = params if (params) else {})
            result = [dict(row) for row in result_it.mappings().all()] if (with_result) else []
            
            conn.commit()

        return result
