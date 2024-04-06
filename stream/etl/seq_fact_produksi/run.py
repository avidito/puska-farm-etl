from etl.helper import (
    db,
    id_getter,
    log,
    kafka,
    validator,
)

from etl.seq_fact_produksi.modules.entity import FactProduksiID, InputProduksi
from etl.seq_fact_produksi.modules.repository import FactProduksiRepository
from etl.seq_fact_produksi.modules.usecase import FactProduksiUsecase


# Main Sequence
def main(
    data: dict,
    validator_h: validator.ValidatorHelper,
    id_getter_h: id_getter.IDGetterHelper,
    log_stream_h: log.LogStreamHelper,
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
        event_data: InputProduksi = validator_h.validate(data)
        
        log_stream_h.start_log("fact_produksi", event_data.source_table, event_data.action, event_data.data)
        fact_produksi_id = FactProduksiID(
            id_waktu = id_getter_h.get_id_waktu(event_data.data.tgl_produksi),
            id_lokasi = id_getter_h.get_id_lokasi_from_unit_ternak(event_data.data.id_unit_ternak),
            id_unit_peternak = event_data.data.id_unit_ternak,
            id_jenis_produk = event_data.data.id_jenis_produk,
            id_sumber_pasokan = id_getter_h.get_id_sumber_pasokan(event_data.data.sumber_pasokan)
        )
        usecase.load(event_data, fact_produksi_id)
        
        log_stream_h.end_log()
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()
    validator_h = validator.ValidatorHelper(logger, InputProduksi)

    dwh = db.DWHHelper()
    log_stream_h = log.LogStreamHelper(dwh)
    id_getter_h = id_getter.IDGetterHelper(dwh, logger)
    
    repo = FactProduksiRepository(dwh, logger)
    usecase = FactProduksiUsecase(repo, logger)

    kafka_h = kafka.KafkaHelper("seq_fact_produksi", logger)
    kafka_h.run(
        main,
        validator_h = validator_h,
        id_getter_h = id_getter_h,
        log_stream_h = log_stream_h,
        usecase = usecase,
    )