from pydantic import BaseModel
from datetime import date


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

    @classmethod
    def from_calc(cls, c: FactDistribusiCalc, id_waktu: int, id_lokasi: int):
        c_data = c.model_dump()
        c_data.pop("tgl_distribusi")
        
        return cls(
            **c_data,
            id_waktu = id_waktu,
            id_lokasi = id_lokasi,
        )
