from typing import Optional
from pydantic import BaseModel
from datetime import date

# DWH
class FactPopulasi(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_peternakan: int
    jenis_kelamin: str
    tipe_ternak: str
    tipe_usia: str
    jumlah_lahir: int
    jumlah_mati: int
    jumlah_masuk: int
    jumlah_keluar: int
    jumlah: int

# OPS
class FactPopulasiCalc(BaseModel):
    tgl_pencatatan: date
    id_peternakan: int
    jenis_kelamin: str
    tipe_ternak: str
    tipe_usia: str
    jumlah_lahir: int = 0
    jumlah_mati: int = 0
    jumlah_masuk: int = 0
    jumlah_keluar: int = 0
    jumlah: int = 0


# Input
class ParamsFactPopulasi(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
