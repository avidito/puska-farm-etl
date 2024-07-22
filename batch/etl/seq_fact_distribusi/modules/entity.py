from typing import Optional
from pydantic import BaseModel
from datetime import date


# OPS
class FactDistribusiCalc(BaseModel):
    tgl_distribusi: date
    id_unit_peternakan: int
    id_jenis_produk: int
    id_mitra_bisnis: int
    jumlah_distribusi: Optional[float] = None
    harga_minimum: Optional[int] = None
    harga_maximum: Optional[int] = None
    harga_rata_rata: Optional[float] = None
    jumlah_penjualan: Optional[float] = None
