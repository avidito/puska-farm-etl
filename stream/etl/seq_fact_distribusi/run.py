from etl.helper import (
    db,
    log,
    kafka,
)

from etl.seq_fact_distribusi.modules.entity import KafkaDistribusi
from etl.seq_fact_distribusi.modules.repository import (
    FactDistribusiDWHRepository,
)
from etl.seq_fact_distribusi.modules.usecase import FactDistribusiUsecase


# Main Sequence
def main(ev_data: KafkaDistribusi, distribusi_usecase: FactDistribusiUsecase):
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
        # Create/Update DWH
        fact_distribusi = distribusi_usecase.get_or_create(
            tgl_distribusi = ev_data.data.tgl_distribusi,
            id_unit_ternak = ev_data.data.id_unit_ternak,
            id_jenis_produk = ev_data.data.id_jenis_produk,
            id_mitra_bisnis = ev_data.data.id_mitra_bisnis,
        )
        fact_distribusi = distribusi_usecase.transform(ev_data.data, fact_distribusi)
        distribusi_usecase.load(fact_distribusi)
        
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()

    dwh = db.DWHHelper()
    dwh_repo = FactDistribusiDWHRepository(dwh, logger)
    
    distribusi_usecase = FactDistribusiUsecase(dwh_repo)

    # Setup Runtime
    kafka_h = kafka.KafkaHelper("seq_fact_distribusi", logger)
    kafka_h.run(main, Validator=KafkaDistribusi, distribusi_usecase=distribusi_usecase)
