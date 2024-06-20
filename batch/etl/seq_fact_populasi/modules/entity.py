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
    jumlah_lahir: Optional[int] = None
    jumlah_mati: Optional[int] = None
    jumlah_masuk: Optional[int] = None
    jumlah_keluar: Optional[int] = None
    jumlah: Optional[int] = None

# OPS
class FactPopulasiCalc(BaseModel):
    tgl_pencatatan: date
    id_peternakan: int
    jenis_kelamin: str
    tipe_ternak: str
    tipe_usia: str
    jumlah_lahir: Optional[int] = None
    jumlah_mati: Optional[int] = None
    jumlah_masuk: Optional[int] = None
    jumlah_keluar: Optional[int] = None
    jumlah: Optional[int] = None


# Input
class ParamsFactPopulasi(BaseModel):
    start_date: Optional[str] = None
    end_date: Optional[str] = None
