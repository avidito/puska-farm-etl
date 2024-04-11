from pydantic import BaseModel
from datetime import date


# IDs
class FactProduksiID(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    id_jenis_produk: int
    id_sumber_pasokan: int


# DWH
class FactProduksi(FactProduksiID):
    jumlah_produksi: int


# OPS
class Produksi(BaseModel):
    tgl_produksi: date
    id_unit_ternak: int
    id_jenis_produk: int
    sumber_pasokan: str
    jumlah: int


# Kafka
class KafkaProduksi(BaseModel):
    source_table: str
    action: str
    data: Produksi
