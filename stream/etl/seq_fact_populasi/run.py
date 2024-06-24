from etl.helper import (
    db,
    log,
    kafka,
)

from etl.seq_fact_populasi.modules.entity import KafkaPopulasi
from etl.seq_fact_populasi.modules.repository import (
    FactPopulasiDWHRepository,
)
from etl.seq_fact_populasi.modules.usecase import FactPopulasiUsecase


# Main Sequence
def main(ev_data: KafkaPopulasi, populasi_usecase: FactPopulasiUsecase):
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
        # Create/Update DWH
        fact_populasi = populasi_usecase.get_or_create(
            tgl_pencatatan = ev_data.data.tgl_pencatatan,
            id_peternak = ev_data.data.id_peternak,
        )
        fact_populasi = populasi_usecase.transform(ev_data.data, fact_populasi)
        populasi_usecase.load(fact_populasi)
        
        logger.info("Processed - Status: OK")
    except Exception as err:
        logger.error(str(err))
        logger.info("Processed - Status: FAILED")


# Runtime
if __name__ == "__main__":
    logger = log.create_logger()

    dwh = db.DWHHelper()
    dwh_repo = FactPopulasiDWHRepository(dwh, logger)
    
    populasi_usecase = FactPopulasiUsecase(dwh_repo)

    # Setup Runtime
    kafka_h = kafka.KafkaHelper("seq_fact_populasi", logger)
    kafka_h.run(main, Validator=KafkaPopulasi, populasi_usecase = populasi_usecase)
