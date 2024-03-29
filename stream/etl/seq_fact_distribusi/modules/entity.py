from pydantic import BaseModel
from datetime import date

# DWH
class FactDistribusi(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    id_mitra_bisnis: int
    id_jenis_produk: int
    jumlah_distribusi: int
    harga_minimum: int
    harga_maximum: int
    harga_rata_rata: float
    jumlah_penjualan: int

# OPS
class Distribusi(BaseModel):
    tgl_distribusi: date
    id_unit_ternak: int
    id_jenis_produk: int
    id_mitra_bisnis: int
    jumlah: int
    harga_berlaku: int

# Input
class InputDistribusi(BaseModel):
    source_table: str
    action: str
    data: Distribusi

# IDs
class FactDistribusiID(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    id_mitra_bisnis: int
    id_jenis_produk: int
