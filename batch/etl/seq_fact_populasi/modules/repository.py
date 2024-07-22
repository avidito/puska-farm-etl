import os
from typing import List
from sqlalchemy import Connection

from etl.helper.db import PostgreSQLHelper
from etl.helper.log import LogBatch

from etl.seq_fact_populasi.modules.entity import (
    FactPopulasiCalc,
)


class FactPopulasiDWHRepository:
    __dwh: PostgreSQLHelper
    __etl_query_dir: str

    def __init__(self, dwh: PostgreSQLHelper):
        self.__dwh = dwh
        self.__etl_query_dir = os.path.join(os.path.dirname(__file__), "query")


    # Public
    def load_log(self, log: LogBatch) -> int:
        self.__dwh.load(
            table_name = "log_batch",
            data = [log],
        )

    def update_dwh(self, calc: List[FactPopulasiCalc]) -> float:
        db = self.__dwh.get_db()
        with db.connect() as conn:
            processed_rows = self.__create_tmp_table(conn, calc)
            self.__update_dwh(conn)

            conn.commit()
        return processed_rows


    # Private
    def __create_tmp_table(self, conn: Connection, calc: List[FactPopulasiCalc]) -> int:
        self.__dwh.run_query_session(conn, self.__etl_query_dir, "create_tmp_fact_populasi")
        processed_rows = self.__dwh.load_session(
            conn,
            table_name = "tmp_fact_populasi",
            data = calc,
        )
        return processed_rows


    def __update_dwh(self, conn: Connection):
        self.__dwh.run_query_session(conn, self.__etl_query_dir, "update_fact_populasi")
    

class FactPopulasiOpsRepository:
    __ops: PostgreSQLHelper
    __etl_query_dir: str

    def __init__(self, ops: PostgreSQLHelper):
        self.__ops = ops
        self.__etl_query_dir = os.path.join(os.path.dirname(__file__), "query")
    

    # Public - CDC
    def copy_cdc(self, table: str):
        self.__ops.copy_cdc(table)


    def remove_cdc(self, table: str):
        self.__ops.remove_cdc(table)


    def flag_cdc(self, table: str):
        self.__ops.flag_cdc(table)


    # Public
    def transform(self) -> FactPopulasiCalc:
        fact_distribusi_calc = [
            FactPopulasiCalc.model_validate(d)
            for d in self.__ops.get_data(self.__etl_query_dir, "transform_fact_populasi")
        ]

        return fact_distribusi_calc
