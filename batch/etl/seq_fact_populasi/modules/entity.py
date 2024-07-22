from typing import Optional
from pydantic import BaseModel
from datetime import date


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
