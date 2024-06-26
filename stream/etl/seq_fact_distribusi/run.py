from etl.helper import (
    db,
    log,
    kafka,
    websocket,
)

from etl.seq_fact_distribusi.modules.entity import KafkaDistribusi
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
    FactDistribusiWebSocketRepository,
)
from etl.seq_fact_distribusi.modules.usecase import FactDistribusiUsecase


# Main Sequence
def main(ev_data: KafkaDistribusi, distribusi_usecase: FactDistribusiUsecase, stream_logger: log.LogStreamHelper):
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
    # Start Logger
    stream_logger.start_log("fact_distribusi", ev_data.source_table, ev_data.action, ev_data.data)

    try:
        # Create/Update DWH
        fact_distribusi = distribusi_usecase.get_or_create(
            tgl_distribusi = ev_data.data.tgl_distribusi,
            id_unit_ternak = ev_data.data.id_unit_ternak,
            id_jenis_produk = ev_data.data.id_jenis_produk,
            id_mitra_bisnis = ev_data.data.id_mitra_bisnis,
        )
        fact_distribusi = distribusi_usecase.transform(ev_data.data, fact_distribusi)
        distribusi_usecase.load(fact_distribusi)

        # Push WebSocket
        distribusi_usecase.push_websocket()
        
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
    dwh_repo = FactDistribusiDWHRepository(dwh, logger)

    ws = websocket.WebSocketHelper()
    ws_repo = FactDistribusiWebSocketRepository(ws, logger)
    
    distribusi_usecase = FactDistribusiUsecase(dwh_repo, ws_repo)

    # Setup Runtime
    kafka_h = kafka.KafkaHelper("seq_fact_distribusi", logger)
    stream_logger = log.LogStreamHelper(dwh)
    kafka_h.run(main, Validator=KafkaDistribusi, distribusi_usecase = distribusi_usecase, stream_logger = stream_logger)
