from etl.helper import (
    db,
    id_getter,
    log,
    kafka,
    validator,
)

from etl.seq_fact_produksi.modules.entity import (
    FactProduksiID,
    KafkaProduksi,
)
from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
)
from etl.seq_fact_produksi.modules.usecase import FactProduksiUsecase


# Main Sequence
def main(
    data: dict,
    log_stream_h: log.LogStreamHelper,
    validator_h: validator.ValidatorHelper,
    id_getter_h: id_getter.IDGetterHelper,
    usecase: FactProduksiUsecase
):
    """
    Fact Produksi - Streaming ETL

    Schema:
    {
        "source_table": [str],
        "action": [str],
        "data": {
            "tgl_produksi": [date],
            "id_unit_ternak": [int],
            "id_jenis_produk": [int],
            "id_mitra_bisnis": [int],
            "jumlah": [int]
        }
    }
    """
    
    try:
        event_data: KafkaProduksi = validator_h.validate(data)
        log_stream_h.start_log("fact_produksi", event_data.source_table, event_data.action, event_data.data)

        usecase.load(event_data, FactProduksiID(
            id_waktu = id_getter_h.get_id_waktu(event_data.data.tgl_produksi),
            id_lokasi = id_getter_h.get_id_lokasi_from_unit_peternakan(event_data.data.id_unit_ternak),
            id_unit_peternakan = event_data.data.id_unit_ternak,
            id_jenis_produk = event_data.data.id_jenis_produk,
            id_sumber_pasokan = id_getter_h.get_id_sumber_pasokan(event_data.data.sumber_pasokan)
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
    validator_h = validator.ValidatorHelper(logger, KafkaProduksi)
    id_getter_h = id_getter.IDGetterHelper(dwh, logger)
    
    dwh_repo = FactProduksiDWHRepository(dwh, logger)
    usecase = FactProduksiUsecase(dwh_repo, logger)

    # Setup Runtime
    kafka_h = kafka.KafkaHelper("seq_fact_produksi", logger)
    kafka_h.run(
        main,
        log_stream_h = log_stream_h,
        validator_h = validator_h,
        id_getter_h = id_getter_h,
        usecase = usecase,
    )