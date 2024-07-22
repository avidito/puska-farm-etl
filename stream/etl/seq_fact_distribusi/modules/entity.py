from pydantic import BaseModel
from typing import Union, Optional
from datetime import date


# IDs
class FactDistribusiID(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternakan: int
    id_mitra_bisnis: int
    id_jenis_produk: int


# DWH
class FactDistribusi(FactDistribusiID):
    jumlah_distribusi: float
    harga_minimum: int
    harga_maximum: int
    harga_rata_rata: float
    jumlah_penjualan: float


# OPS
class DistribusiSusu(BaseModel):
    tgl_distribusi: date
    id_unit_ternak: int
    id_jenis_produk: int
    id_mitra_bisnis: int
    jumlah: float
    harga_berlaku: int

class DistribusiTernak(BaseModel):
    tgl_distribusi: date
    id_unit_ternak: int
    id_jenis_produk: int
    id_mitra_bisnis: int
    jumlah: float
    harga_berlaku: int


# Kafka
class KafkaDistribusi(BaseModel):
    source_table: str
    action: str
    old_data: Optional[Union[
        DistribusiSusu,
        DistribusiTernak
    ]]
    new_data: Optional[Union[
        DistribusiSusu,
        DistribusiTernak
    ]]
