import sys
from etl.helper import (
    db,
    id_getter,
    log,
)

from etl.seq_fact_produksi.modules.entity import (
    FactProduksi,
    ParamsFactProduksi,
)
from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
    FactProduksiOpsRepository,
)
from etl.seq_fact_produksi.modules.usecase import FactProduksiUsecase


# Main Sequence
def main(
    params: ParamsFactProduksi,
    log_batch_h: log.LogBatchHelper,
    id_getter_h: id_getter.IDGetterHelper,
    usecase: FactProduksiUsecase,
):
    """
    Fact Produksi - Batch ETL

    Params:
        - START_DATE [str]. Start date of data processing in format '%Y-%m-%d' (default: 7 days ago)
        - END_DATE [str]. End date of data processing (include) in format '%Y-%m-%d' (default: today)
    """
    
    try:
        log_batch_h.start_log("fact_produksi", params)
        
        fact_produksi_calc = usecase.get(params)
        fact_produksi = [
            FactProduksi(
                id_waktu = id_getter_h.get_id_waktu(row.tgl_produksi),
                id_lokasi = id_getter_h.get_id_lokasi_from_unit_ternak(row.id_unit_peternak),
                id_unit_peternak = row.id_unit_peternak,
                id_jenis_produk = row.id_jenis_produk,
                id_sumber_pasokan = id_getter_h.get_id_sumber_pasokan(row.sumber_pasokan),
                jumlah_produksi = row.jumlah_produksi,
            )
            for row in fact_produksi_calc
        ]
        processed_row_count = usecase.load(fact_produksi)
        
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
    
    dwh_repo = FactProduksiDWHRepository(dwh, logger)
    ops_repo = FactProduksiOpsRepository(ops, logger)
    usecase = FactProduksiUsecase(dwh_repo, ops_repo, logger)

    # Get runtime params
    _, *sys_params = sys.argv
    sys_params = sys_params + [None] * (2-len(sys_params))
    params = ParamsFactProduksi(
        start_date = sys_params[0],
        end_date = sys_params[1],
    )

    main(
        params,
        log_batch_h = log_batch_h,
        id_getter_h = id_getter_h,
        usecase = usecase,
    )
