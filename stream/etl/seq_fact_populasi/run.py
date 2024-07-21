import traceback

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
    stream_logger.start_log("fact_populasi", ev_data.source_table, ev_data.action, ev_data.old_data, ev_data.new_data)
    
    try:
        # Create/Update DWH
        fact_populasi_map = populasi_usecase.get_or_create(
            tgl_pencatatan = ev_data.new_data.tgl_pencatatan if (ev_data.new_data) else ev_data.old_data.tgl_pencatatan,
            id_peternak = ev_data.new_data.id_peternak if (ev_data.new_data) else ev_data.old_data.id_peternak,
        )
        for fact_populasi in fact_populasi_map.values():
            if ev_data.source_table == "history_populasi":
                fact_populasi = populasi_usecase.transform_jumlah(ev_data.action, ev_data.old_data, ev_data.new_data, fact_populasi)
            elif ev_data.source_table == "history_kelahiran_kematian":
                fact_populasi = populasi_usecase.transform_kelahiran_kematian(ev_data.action, ev_data.old_data, ev_data.new_data, fact_populasi)
            elif ev_data.source_table == "pencatatan_ternak_masuk":
                fact_populasi = populasi_usecase.transform_masuk(ev_data.action, ev_data.old_data, ev_data.new_data, fact_populasi)
            elif ev_data.source_table == "pencatatan_ternak_keluar":
                fact_populasi = populasi_usecase.transform_keluar(ev_data.action, ev_data.old_data, ev_data.new_data, fact_populasi)

            populasi_usecase.load(fact_populasi)
        
        # Push WebSocket
        populasi_usecase.push_websocket()
        
        logger.info("Processed - Status: OK")
    except Exception:
        logger.error(traceback.format_exc())
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
