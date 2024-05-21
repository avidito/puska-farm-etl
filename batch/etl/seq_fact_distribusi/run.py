import sys
import pytz
from datetime import datetime, timedelta
from etl.helper import (
    db,
    id_getter,
    log,
)

from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusi,
    ParamsFactDistribusi,
)
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
    FactDistribusiOpsRepository,
)
from etl.seq_fact_distribusi.modules.usecase import FactDistribusiUsecase


# Main Sequence
def main(
    params: ParamsFactDistribusi,
    log_batch_h: log.LogBatchHelper,
    id_getter_h: id_getter.IDGetterHelper,
    usecase: FactDistribusiUsecase,
):
    """
    Fact Distribusi - Batch ETL

    Params:
        - START_DATE [str]. Start date of data processing in format '%Y-%m-%d' (default: 7 days ago)
        - END_DATE [str]. End date of data processing (include) in format '%Y-%m-%d' (default: today)
    """
    
    try:
        log_batch_h.start_log("fact_distribusi", params)
        
        fact_distribusi_calc = usecase.get(params)
        fact_distribusi = [
            FactDistribusi(
                id_waktu = id_getter_h.get_id_waktu(row.tgl_distribusi),
                id_lokasi = id_getter_h.get_id_lokasi_from_unit_ternak(row.id_unit_peternak),
                id_unit_peternak = row.id_unit_peternak,
                id_mitra_bisnis = row.id_mitra_bisnis,
                id_jenis_produk = row.id_jenis_produk,
                jumlah_distribusi = row.jumlah_distribusi,
                harga_minimum = row.harga_minimum,
                harga_maximum = row.harga_maximum,
                harga_rata_rata = row.harga_rata_rata,
                jumlah_penjualan = row.jumlah_penjualan,
            )
            for row in fact_distribusi_calc
        ]
        processed_row_count = usecase.load(fact_distribusi)
        
        log_batch_h.end_log(processed_row_count)
        logger.info(f"Processed - Status: OK (Affected: {processed_row_count} row count)")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()
    dwh = db.DWHHelper()
    ops = db.OpsHelper()

    log_batch_h = log.LogBatchHelper(dwh)
    id_getter_h = id_getter.IDGetterHelper(dwh, logger)
    
    dwh_repo = FactDistribusiDWHRepository(dwh, logger)
    ops_repo = FactDistribusiOpsRepository(ops, logger)
    usecase = FactDistribusiUsecase(dwh_repo, ops_repo, logger)

    # Get runtime params
    _, *sys_params = sys.argv
    start_date, end_date = sys_params + [None] * (2-len(sys_params))
    start_date = start_date if (start_date) else (datetime.now(pytz.timezone("Asia/Jakarta")) - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = end_date if (end_date) else datetime.now(pytz.timezone("Asia/Jakarta")).strftime("%Y-%m-%d")

    params = ParamsFactDistribusi(
        start_date = start_date,
        end_date = end_date,
    )

    main(
        params,
        log_batch_h = log_batch_h,
        id_getter_h = id_getter_h,
        usecase = usecase,
    )
