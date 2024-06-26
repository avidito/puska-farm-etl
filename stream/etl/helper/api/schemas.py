from pydantic import BaseModel


class MLTriggerProduksi(BaseModel):
    time_type: str = "Daily"
    id_waktu: int
    id_lokasi: int
    id_unit_peternakan: int