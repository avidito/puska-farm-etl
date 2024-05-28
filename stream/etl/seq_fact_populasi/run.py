import asyncio

from etl.helper import (
    db,
    id_getter,
    log,
    kafka,
    validator,
    websocket,
)
from etl.helper.config import CONFIG

from etl.seq_fact_populasi.modules.entity import (
    FactPopulasiID,
    KafkaPopulasi,
)
from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
)
from etl.seq_fact_populasi.modules.usecase import FactPopulasiUsecase


# Main Sequence
def main(
    data: dict,
    log_stream_h: log.LogStreamHelper,
    validator_h: validator.ValidatorHelper,
    id_getter_h: id_getter.IDGetterHelper,
    websocket_h: websocket.WebSocketHelper,
    usecase: FactPopulasiUsecase
):
    """
    Fact Populasi - Streaming ETL

    Schema:
    {
        "source_table": [str],
        "action": [str],
        "data": {
            "tgl_pencatatan": [date],
            "id_peternak": [int],
            "jml_pedaging_jantan": [int],
            "jml_pedaging_betina": [int],
            "jml_pedaging_anakan_jantan": [int],
            "jml_pedaging_anakan_betina": [int],
            "jml_perah_jantan": [int],
            "jml_perah_betina": [int],
            "jml_perah_anakan_jantan": [int],
            "jml_perah_anakan_betina": [int],
        }
    }
    """
    
    try:
        event_data: KafkaPopulasi = validator_h.validate(data)
        log_stream_h.start_log("fact_populasi", event_data.source_table, event_data.action, event_data.data)
        
        usecase.load(event_data, FactPopulasiID(
            id_waktu = id_getter_h.get_id_waktu(event_data.data.tgl_pencatatan),
            id_lokasi = id_getter_h.get_id_lokasi_from_peternakan(event_data.data.id_peternak),
            id_peternakan = event_data.data.id_peternak,
        ))
        
        asyncio.run(websocket_h.send_message({"type": "etl-susu"}))
        asyncio.run(websocket_h.send_message({"type": "etl-ternak"}))
        
        log_stream_h.end_log()
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()
    dwh = db.DWHHelper()

    dwh_repo = FactPopulasiDWHRepository(dwh, logger)
    usecase = FactPopulasiUsecase(dwh_repo, logger)

    # Setup Runtime
    log_stream_h = log.LogStreamHelper(dwh)
    validator_h = validator.ValidatorHelper(logger, KafkaPopulasi)
    id_getter_h = id_getter.IDGetterHelper(dwh, logger)
    websocket_h = websocket.WebSocketHelper()
    
    kafka_h = kafka.KafkaHelper("seq_fact_populasi", logger)
    kafka_h.run(
        main,
        log_stream_h = log_stream_h,
        validator_h = validator_h,
        id_getter_h = id_getter_h,
        websocket_h = websocket_h,
        usecase = usecase,
    )