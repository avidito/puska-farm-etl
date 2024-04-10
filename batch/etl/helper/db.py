import os
from typing import List, Optional
from datetime import date
from pydantic import BaseModel

from sqlalchemy import Engine, create_engine, text
from urllib.parse import quote_plus

from etl.helper.config import CONFIG


# Getter
def get_dwh() -> Engine:
    engine = create_engine(
        "{dialect}://{username}:{password}@{hostname}:{port}/{name}".format(
            dialect = CONFIG.DWH_DIALECT,
            username = CONFIG.DWH_USERNAME,
            password = quote_plus(CONFIG.DWH_PASSWORD.get_secret_value()),
            hostname = CONFIG.DWH_HOSTNAME,
            port = CONFIG.DWH_PORT,
            name = CONFIG.DWH_NAME,
        )
    )
    return engine

def get_ops() -> Engine:
    engine = create_engine(
        "{dialect}://{username}:{password}@{hostname}:{port}/{name}".format(
            dialect = CONFIG.OPS_DIALECT,
            username = CONFIG.OPS_USERNAME,
            password = quote_plus(CONFIG.OPS_PASSWORD.get_secret_value()),
            hostname = CONFIG.OPS_HOSTNAME,
            port = CONFIG.OPS_PORT,
            name = CONFIG.OPS_NAME,
        )
    )
    return engine


# Helper
class DWHHelper:
    __db: Engine

    def __init__(self):
        self.__db = get_dwh()
    

    # Methods
    def exec(self, query_str: str, params: dict) -> List[dict]:
        query_clause = text(query_str)
        with self.__db.connect() as conn:
            results_it = conn.execute(query_clause, params)
            results = [dict(row) for row in results_it.mappings().all()]
            conn.commit()
        return results


    def run(self, query_dir: str, query_file: str, params: dict) -> Optional[List[dict]]:
        with open(os.path.join(query_dir, query_file), mode="r") as file:
            query = file.read()
        results = self.exec(query, params)
        return results


    def load(self, table: str, data: List[BaseModel], pk: List[str], update_insert: bool = False) -> int:
        if (len(data) > 0):
            data_dict = [row.model_dump() for row in data]
            columns = data_dict[0].keys()

            load_query = self.__generate_load_query(table, columns, pk, data_dict, update_insert)
            processed_rows = self.exec(load_query, {})[0]["processed_rows"]
            return processed_rows
        else:
            return 0
    

    # Private
    def __generate_load_query(
        self,
        table: str,
        columns: List[str],
        pk: List[str],
        data: List[dict],
        update_insert: bool
    ) -> str:
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
                    insert_value.append(f"'{row[col]}'")
            
            insert_value += [
                "TIMEZONE('Asia/Jakarta', NOW())",
                "TIMEZONE('Asia/Jakarta', NOW())"
            ]

            insert_value_rows.append(f"({','.join(insert_value)})")
        
        insert_value_statement = ", ".join(insert_value_rows)

        # Define Update/Insert Statement
        upsert_statement = "\n".join([
            f"  ON CONFLICT ON CONSTRAINT {table}_pkey DO UPDATE SET",
            *[
                f"    {col} = EXCLUDED.{col},"
                for col in filter(lambda c: c not in pk, columns)
            ],
            "    modified_dt = TIMEZONE('Asia/Jakarta', NOW())",
        ])

        query = insert_statement + insert_value_statement
        if (update_insert):
            query = "\n".join([
                query,
                upsert_statement,
            ])
        
        # Get Row Count
        query = "\n".join([
            "WITH data_load AS (",
            query,
            "  RETURNING 1",
            ")",
            "SELECT COUNT(0) AS processed_rows FROM data_load;",
        ])
        return query


class OpsHelper:
    __db: Engine

    def __init__(self):
        self.__db = get_ops()
    

    # Methods
    def exec(self, query_str: str, params: dict) -> List[dict]:
        query_clause = text(query_str)
        with self.__db.connect() as conn:
            results_it = conn.execute(query_clause, params)
            results = [dict(row) for row in results_it.mappings().all()]
        return results


    def run(self, query_dir: str, query_file: str, params: dict) -> Optional[List[dict]]:
        with open(os.path.join(query_dir, query_file), mode="r") as file:
            query = file.read()
        results = self.exec(query, params)
        return results
