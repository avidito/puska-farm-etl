from datetime import timedelta
from etl.helper import (
    api,
    db,
    log,
    kafka,
    websocket,
)

from etl.seq_fact_produksi.modules.entity import KafkaProduksi
from etl.seq_fact_produksi.modules.repository import (
    FactProduksiDWHRepository,
    FactProduksiMLRepository,
    FactProduksiWebSocketRepository,
)
from etl.seq_fact_produksi.modules.usecase import FactProduksiUsecase


# Main Sequence
def main(ev_data: KafkaProduksi, produksi_usecase: FactProduksiUsecase, stream_logger: log.LogStreamHelper):
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
    # Start Logger
    logger.info(f"Start processing new data '{ev_data.action} - {ev_data.source_table}'")
    stream_logger.start_log("fact_produksi", ev_data.source_table, ev_data.action, ev_data.old_data, ev_data.new_data)
    
    try:
        # Create/Update DWH
        tgl_produksi = ev_data.new_data.tgl_produksi if (ev_data.new_data) else ev_data.old_data.tgl_produksi
        
        fact_produksi = produksi_usecase.get_or_create(
            tgl_produksi = tgl_produksi,
            id_unit_ternak = ev_data.new_data.id_unit_ternak if (ev_data.new_data) else ev_data.old_data.id_unit_ternak,
            id_jenis_produk = ev_data.new_data.id_jenis_produk if (ev_data.new_data) else ev_data.old_data.id_jenis_produk,
            sumber_pasokan = ev_data.new_data.sumber_pasokan if (ev_data.new_data) else ev_data.old_data.sumber_pasokan,
        )

        if ev_data.source_table == "produksi_susu":
            fact_produksi = produksi_usecase.transform_susu(ev_data.action, ev_data.old_data, ev_data.new_data, fact_produksi)
        elif ev_data.source_table == "produksi_ternak":
            fact_produksi = produksi_usecase.transform_ternak(ev_data.action, ev_data.old_data, ev_data.new_data, fact_produksi)
        
        produksi_usecase.load(fact_produksi)

        # Trigger ML API
        if (ev_data.source_table == "produksi_susu") and (fact_produksi.id_jenis_produk == 3):
            produksi_usecase.predict_susu(
                tgl_prediksi = tgl_produksi + timedelta(days=1),
                id_unit_peternakan = fact_produksi.id_unit_peternakan,
                id_lokasi = fact_produksi.id_lokasi,
            )

        # Push WebSocket
        produksi_usecase.push_websocket()


        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")
        raise err
    
    # End Logger
    logger.info(f"Finish processing new data '{ev_data.action} - {ev_data.source_table}'")
    stream_logger.end_log()


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()
    
    dwh = db.DWHHelper()
    dwh_repo = FactProduksiDWHRepository(dwh, logger)
    
    ml_api = api.MLAPIHelper()
    ml_repo = FactProduksiMLRepository(ml_api, logger)

    ws = websocket.WebSocketHelper()
    ws_repo = FactProduksiWebSocketRepository(ws, logger)

    produksi_usecase = FactProduksiUsecase(dwh_repo, ml_repo, ws_repo)

    # Setup Runtime
    kafka_h = kafka.KafkaHelper("seq_fact_produksi", logger)
    stream_logger = log.LogStreamHelper(dwh)
    kafka_h.run(main, Validator=KafkaProduksi, produksi_usecase = produksi_usecase, stream_logger=stream_logger)
