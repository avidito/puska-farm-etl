from etl.helper import (
    db,
    id_getter,
    log,
    kafka,
    validator,
)

from etl.seq_fact_distribusi.modules.entity import FactDistribusiID, InputDistribusi
from etl.seq_fact_distribusi.modules.repository import FactDistribusiRepository
from etl.seq_fact_distribusi.modules.usecase import FactDistribusiUsecase


# Main Sequence
def main(
    data: dict,
    validator_h: validator.ValidatorHelper,
    id_getter_h: id_getter.IDGetterHelper,
    log_stream_h: log.LogStreamHelper,
    usecase: FactDistribusiUsecase
):
    """
    Fact Distribusi - Streaming ETL

    Schema:
    {
        "source_table": [str],
        "action": [str],
        "data": {
            "tgl_distribusi": [date],
            "id_unit_ternak": [int],
            "id_jenis_produk": [int],
            "id_mitra_bisnis": [int],
            "jumlah": [int],
            "harga_berlaku": [int]
        }
    }
    """
    
    try:
        event_data: InputDistribusi = validator_h.validate(data)
        
        log_stream_h.start_log("fact_distribusi", event_data.source_table, event_data.action, event_data.data)
        fact_distribusi_id = FactDistribusiID(
            id_waktu = id_getter_h.get_id_waktu(event_data.data.tgl_distribusi),
            id_lokasi = id_getter_h.get_id_lokasi_from_unit_ternak(event_data.data.id_unit_ternak),
            id_unit_peternak = event_data.data.id_unit_ternak,
            id_mitra_bisnis = event_data.data.id_mitra_bisnis,
            id_jenis_produk = event_data.data.id_jenis_produk,
        )
        usecase.load(event_data, fact_distribusi_id)
        
        log_stream_h.end_log()
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()
    validator_h = validator.ValidatorHelper(logger, InputDistribusi)

    dwh = db.DWHHelper()
    id_getter_h = id_getter.IDGetterHelper(dwh)
    log_stream_h = log.LogStreamHelper(dwh)
    
    repo = FactDistribusiRepository(dwh, logger)
    usecase = FactDistribusiUsecase(repo, logger)

    kafka_h = kafka.KafkaHelper("seq_fact_distribusi", logger)
    kafka_h.run(
        main,
        validator_h = validator_h,
        id_getter_h = id_getter_h,
        log_stream_h = log_stream_h,
        usecase = usecase,
    )