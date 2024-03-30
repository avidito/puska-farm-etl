from pydantic import BaseModel, ConfigDict
from typing import Optional
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
class HistoryPopulasi(BaseModel):
    tgl_pencatatan: date
    id_peternak: int
    jml_pedaging_jantan: int
    jml_pedaging_betina: int
    jml_pedaging_anakan_jantan: int
    jml_pedaging_anakan_betina: int
    jml_perah_jantan: int
    jml_perah_betina: int
    jml_perah_anakan_jantan: int
    jml_perah_anakan_betina: int

# Input
class InputPopulasi(BaseModel):
    source_table: str
    action: str
    data: HistoryPopulasi

# IDs
class FactPopulasiID(BaseModel):
    id_waktu: int
    id_lokasi: int
    id_peternakan: int
    jenis_kelamin: Optional[str] = None
    tipe_ternak: Optional[str] = None
    tipe_usia: Optional[str] = None

    model_config = ConfigDict(validate_assignment=True)
