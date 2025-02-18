from pydantic import BaseModel
from typing import Optional, Union
from datetime import date


# IDs
class FactProduksiID(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternakan: int
    id_jenis_produk: int
    id_sumber_pasokan: int


class UnitPeternakanLokasi(BaseModel):
    id_provinsi: int
    label_provinsi: str
    id_kabupaten_kota: int
    label_kabupaten_kota: str


# DWH
class FactProduksi(FactProduksiID):
    jumlah_produksi: float


# OPS
class ProduksiSusu(BaseModel):
    tgl_produksi: date
    id_unit_ternak: int
    id_jenis_produk: int
    sumber_pasokan: str
    jumlah: float


class ProduksiTernak(BaseModel):
    tgl_produksi: date
    id_unit_ternak: int
    id_jenis_produk: int
    sumber_pasokan: str
    jumlah: float


# Kafka
class KafkaProduksi(BaseModel):
    source_table: str
    action: str
    old_data: Optional[Union[
        ProduksiSusu,
        ProduksiTernak
    ]]
    new_data: Optional[Union[
        ProduksiSusu,
        ProduksiTernak
    ]]
