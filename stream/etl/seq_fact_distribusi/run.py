from etl.helper import (
    db,
    id_getter,
    log,
    kafka,
    validator,
)

from etl.seq_fact_distribusi.modules.entity import (
    FactDistribusiID,
    KafkaDistribusi,
)
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
)
from etl.seq_fact_distribusi.modules.usecase import FactDistribusiUsecase


# Main Sequence
def main(
    data: dict,
    log_stream_h: log.LogStreamHelper,
    validator_h: validator.ValidatorHelper,
    id_getter_h: id_getter.IDGetterHelper,
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
        event_data: KafkaDistribusi = validator_h.validate(data)
        log_stream_h.start_log("fact_distribusi", event_data.source_table, event_data.action, event_data.data)
        
        usecase.load(event_data, FactDistribusiID(
            id_waktu = id_getter_h.get_id_waktu(event_data.data.tgl_distribusi),
            id_lokasi = id_getter_h.get_id_lokasi_from_unit_peternakan(event_data.data.id_unit_ternak),
            id_unit_peternakan = event_data.data.id_unit_ternak,
            id_mitra_bisnis = event_data.data.id_mitra_bisnis,
            id_jenis_produk = event_data.data.id_jenis_produk,
        ))
        
        log_stream_h.end_log()
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()
    dwh = db.DWHHelper()

    log_stream_h = log.LogStreamHelper(dwh)
    validator_h = validator.ValidatorHelper(logger, KafkaDistribusi)
    id_getter_h = id_getter.IDGetterHelper(dwh, logger)
    
    dwh_repo = FactDistribusiDWHRepository(dwh, logger)
    usecase = FactDistribusiUsecase(dwh_repo, logger)

    kafka_h = kafka.KafkaHelper("seq_fact_distribusi", logger)
    kafka_h.run(
        main,
        log_stream_h = log_stream_h,
        validator_h = validator_h,
        id_getter_h = id_getter_h,
        usecase = usecase,
    )