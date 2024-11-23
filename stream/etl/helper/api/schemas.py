from typing import Optional

from pydantic import BaseModel


class MLTriggerProduksi(BaseModel):
    time_type: str = "daily"
    id_waktu: int
    id_lokasi: int
    id_unit_peternakan: Optional[int] = None
