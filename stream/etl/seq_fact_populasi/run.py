from etl.helper import (
    db,
    log,
    kafka,
    websocket,
)

from etl.seq_fact_populasi.modules.entity import KafkaPopulasi
from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
    FactPopulasiWebSocketRepository,
)
from etl.seq_fact_populasi.modules.usecase import FactPopulasiUsecase


# Main Sequence
def main(ev_data: KafkaPopulasi, populasi_usecase: FactPopulasiUsecase, stream_logger: log.LogStreamHelper):
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
    # Start Logger
    stream_logger.start_log("fact_populasi", ev_data.source_table, ev_data.action, ev_data.data)
    
    try:
        # Create/Update DWH
        fact_populasi_hash = populasi_usecase.get_or_create(
            tgl_pencatatan = ev_data.data.tgl_pencatatan,
            id_peternak = ev_data.data.id_peternak,
        )
        for fact_populasi in fact_populasi_hash.values():
            fact_populasi = populasi_usecase.transform_jumlah(ev_data.data, fact_populasi)
            populasi_usecase.load(fact_populasi)
        
        # Push WebSocket
        populasi_usecase.push_websocket()
        
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")
    
    # End Logger
    stream_logger.end_log()


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()

    dwh = db.DWHHelper()
    dwh_repo = FactPopulasiDWHRepository(dwh, logger)

    ws = websocket.WebSocketHelper()
    ws_repo = FactPopulasiWebSocketRepository(ws, logger)
    
    populasi_usecase = FactPopulasiUsecase(dwh_repo, ws_repo)

    # Setup Runtime
    kafka_h = kafka.KafkaHelper("seq_fact_populasi", logger)
    stream_logger = log.LogStreamHelper(dwh)
    kafka_h.run(main, Validator=KafkaPopulasi, populasi_usecase = populasi_usecase, stream_logger = stream_logger)
