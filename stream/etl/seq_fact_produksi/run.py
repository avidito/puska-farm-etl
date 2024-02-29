import os
from etl.shared import (
    database,
    kafka,
    log,
    schemas
)
logger = log.create_logger()
QUERY_DIR = os.path.join(os.path.dirname(__file__), "query")


# Main Sequence
def main(data: dict):
    try:
        # Validation
        valid_event: schemas.EventFactProduksi = schemas.validating_event(data, schemas.EventFactProduksi, logger)
        
        # Processing
        data_tr = database.get_dwh_ids(valid_event.identifier.model_dump(), {
            "tgl_produksi": "id_waktu",
            "sumber_pasokan": "id_sumber_pasokan"
        })
        prep_data = __prepare_data(data_tr, valid_event.action, valid_event.amount.model_dump())

        # Update DWH
        database.run_query(
            query_name = "upsert_fact_produksi_stream",
            query_dir = QUERY_DIR,
            params = prep_data.model_dump()
        )
        
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Process
def __prepare_data(data: dict, action: str, amount: dict) -> schemas.TableFactProduksi:
    if (action == "CREATE"):
        jumlah_produksi = amount["jumlah"]
    elif (action == "DELETE"):
        jumlah_produksi = amount["jumlah"] * (-1)
    elif (action == "UPDATE"):
        jumlah_produksi = (amount["jumlah"] - amount["prev_jumlah"])
    
    prep_data = {
        **data,
        "jumlah_produksi": jumlah_produksi
    }
    return schemas.TableFactProduksi(**prep_data)


# Runtime
if __name__ == "__main__":
    kafka.get_stream_source(
        "seq_fact_produksi",
        topic = "produksi",
        host = "kafka:9092",
        process = main,
        logger = logger
    )
