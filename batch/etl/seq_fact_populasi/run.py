import sys
from logging import Logger

from etl.helper.db import PostgreSQLHelper
from etl.helper.log import create_logger

from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
    FactPopulasiOpsRepository
)
from etl.seq_fact_populasi.modules.usecase import FactPopulasiUsecase


# Main Sequence
def main(usecase: FactPopulasiUsecase, logger: Logger):
    """
    Fact Populasi - Batch ETL
    """
    
    logger.info("Start ETL: 'seq_fact_populasi'")
    usecase.log_start()

    usecase.copy_source_cdc()
    processed_rows = usecase.transform()
    usecase.flag_cdc()
    
    duration = usecase.log_end(processed_rows)
    logger.info(f"Finish ETL: 'seq_fact_populasi'. Duration: {duration:.4f}s")


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
    dwh_repo = FactPopulasiDWHRepository(dwh)
    ops_repo = FactPopulasiOpsRepository(ops)
    usecase = FactPopulasiUsecase(ops_repo, dwh_repo)

    # Main
    main(usecase, logger)
