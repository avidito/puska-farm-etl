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
        valid_event: schemas.EventFactDistribusi = schemas.validating_event(data, schemas.EventFactDistribusi, logger)
        
        # Processing
        data_tr = database.get_dwh_ids(valid_event.identifier.model_dump(), {
            "tgl_distribusi": "id_waktu"
        })
        prep_data = __prepare_data(data_tr, valid_event.action, valid_event.amount.model_dump())

        # Update DWH
        database.run_query(
            query_name = "upsert_fact_distribusi_stream",
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
        jumlah_distribusi = amount["jumlah"]
        jumlah_penjualan = jumlah_distribusi * amount["harga_berlaku"]
    elif (action == "DELETE"):
        jumlah_distribusi = amount["jumlah"] * (-1)
        jumlah_penjualan = jumlah_distribusi * amount["harga_berlaku"]
    elif (action == "UPDATE"):
        jumlah_distribusi = amount["jumlah"] - amount["prev_jumlah"]
        jumlah_penjualan = (amount["jumlah"] * amount["harga_berlaku"]) - (amount["prev_jumlah"] * amount["prev_harga_berlaku"])
    
    prep_data = {
        **data,
        "jumlah_distribusi": jumlah_distribusi,
        "jumlah_penjualan": jumlah_penjualan
    }
    return schemas.TableFactDistribusi(**prep_data)


# Runtime
if __name__ == "__main__":
    kafka.get_stream_source(
        "seq_fact_distribusi",
        topic = "distribusi",
        host = "localhost:29200",
        process = main,
        logger = logger
    )