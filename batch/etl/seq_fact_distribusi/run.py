import sys
from logging import Logger

from etl.helper.db import PostgreSQLHelper
from etl.helper.log import create_logger
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
    FactDistribusiOpsRepository,
)
from etl.seq_fact_distribusi.modules.usecase import FactDistribusiUsecase


# Main Sequence
def main(usecase: FactDistribusiUsecase, logger: Logger):
    """
    Fact Distribusi - Batch ETL
    """
    logger.info("Start ETL: 'seq_fact_distribusi'")
    usecase.log_start()

    usecase.copy_source_cdc()
    processed_rows = usecase.transform()
    usecase.flag_cdc()

    duration = usecase.log_end(processed_rows)
    logger.info(f"Finish ETL: 'seq_fact_distribusi'. Time: {duration:.4f}s")


# Runtime
if __name__ == "__main__":
    # Get runtime params
    _, *sys_params = sys.argv
    log_levelname = sys_params[0] if (sys_params) else "info"

    logger = create_logger(log_levelname)
    dwh = PostgreSQLHelper(mode = "DWH")
    ops = PostgreSQLHelper(mode = "OPS")

    # log_batch_h = log.LogBatchHelper(dwh)
    
    dwh_repo = FactDistribusiDWHRepository(dwh)
    ops_repo = FactDistribusiOpsRepository(ops)
    usecase = FactDistribusiUsecase(ops_repo, dwh_repo)

    main(usecase, logger)
