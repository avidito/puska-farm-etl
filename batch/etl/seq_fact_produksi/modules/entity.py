from typing import Optional
from pydantic import BaseModel
from datetime import date


# DWH
class FactProduksi(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    id_jenis_produk: int
    id_sumber_pasokan: int
    jumlah_produksi: int


# OPS
class FactProduksiCalc(BaseModel):
    tgl_produksi: date
    id_unit_peternak: int
    id_jenis_produk: int
    sumber_pasokan: str
    jumlah_produksi: int = 0


# Params
class ParamsFactProduksi(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
