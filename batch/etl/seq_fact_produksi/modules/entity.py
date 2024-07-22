from typing import Optional
from pydantic import BaseModel
from datetime import date


# OPS
class FactProduksiCalc(BaseModel):
    tgl_produksi: date
    id_unit_peternakan: int
    id_jenis_produk: int
    sumber_pasokan: str
    jumlah_produksi: Optional[float] = None
