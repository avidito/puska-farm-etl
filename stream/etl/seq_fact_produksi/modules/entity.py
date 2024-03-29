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
class Produksi(BaseModel):
    tgl_produksi: date
    id_unit_ternak: int
    id_jenis_produk: int
    sumber_pasokan: str
    jumlah: int

# Input
class InputProduksi(BaseModel):
    source_table: str
    action: str
    data: Produksi

# IDs
class FactProduksiID(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    id_jenis_produk: int
    id_sumber_pasokan: int
