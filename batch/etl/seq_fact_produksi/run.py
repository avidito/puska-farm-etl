import sys
from logging import Logger

from etl.helper.db import PostgreSQLHelper
from etl.helper.log import create_logger

from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
    FactProduksiOpsRepository,
)
from etl.seq_fact_produksi.modules.usecase import FactProduksiUsecase


# Main Sequence
def main(usecase: FactProduksiUsecase, logger: Logger):
    """
    Fact Produksi - Batch ETL
    """
    logger.info("Start ETL: 'seq_fact_produksi'")
    usecase.log_start()

    usecase.copy_source_cdc()
    processed_rows = usecase.transform()
    usecase.flag_cdc()

    duration = usecase.log_end(processed_rows)
    logger.info(f"Finish ETL: 'seq_fact_produksi'. Duration: {duration:.4f}s")


# Runtime
if __name__ == "__main__":
    # Get runtime params
    _, *sys_params = sys.argv
    log_levelname = sys_params[0] if (sys_params) else "info"

    # Init Helper
    logger = create_logger(log_levelname)
    dwh = PostgreSQLHelper(mode = "DWH")
    ops = PostgreSQLHelper(mode = "OPS")

    # Init Usecase
    dwh_repo = FactProduksiDWHRepository(dwh)
    ops_repo = FactProduksiOpsRepository(ops)
    usecase = FactProduksiUsecase(ops_repo, dwh_repo)

    # Main
    main(usecase, logger)
