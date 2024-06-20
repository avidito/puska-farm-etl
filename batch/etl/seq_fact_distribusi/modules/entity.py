from typing import Optional
from pydantic import BaseModel
from datetime import date


# DWH
class FactDistribusi(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternakan: int
    id_mitra_bisnis: int
    id_jenis_produk: int
    jumlah_distribusi: float
    harga_minimum: int
    harga_maximum: int
    harga_rata_rata: float
    jumlah_penjualan: float


# OPS
class FactDistribusiCalc(BaseModel):
    tgl_distribusi: date
    id_unit_peternakan: int
    id_jenis_produk: int
    id_mitra_bisnis: int
    jumlah_distribusi: float = 0.0
    harga_minimum: int = 0
    harga_maximum: int = 0
    harga_rata_rata: float = 0.0
    jumlah_penjualan: float = 0.0


# Params
class ParamsFactDistribusi(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
