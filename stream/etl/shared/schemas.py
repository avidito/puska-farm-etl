import logging
import pydantic
from typing import Type, Union
from datetime import date


# Validation
def validating_event(data: dict, Base: Type[pydantic.BaseModel], logger: logging.Logger) -> pydantic.BaseModel:
    try:
        return Base(**data)
    except pydantic.ValidationError as err:
        logger.error(str(err))
        raise(err)


"""
Schemas - Produksi
JSON Sample:

##### CREATE #####
# Indented
{
    "source_table": "produksi_susu",
    "action": "CREATE",
    "identifier": {
        "tgl_produksi": "2023-12-23",
        "id_unit_peternak": 1,
        "id_lokasi": 27696,
        "id_jenis_produk": 1,
        "sumber_pasokan": "Pengepul"
    },
    "amount": {
        "jumlah": 10000
    }
}

# Flatten
{"source_table": "produksi", "action": "CREATE", "identifier": {"tgl_produksi": "2023-12-23", "id_unit_peternak": 1, "id_jenis_produk": 1, "id_lokasi": 27696, "sumber_pasokan": "Pengepul"}, "amount": {"jumlah": 10000}}

##### DELETE #####
# Indented
{
    "source_table": "produksi_susu",
    "action": "DELETE",
    "identifier": {
        "tgl_produksi": "2023-12-23",
        "id_unit_peternak": 1,
        "id_lokasi": 27696,
        "id_jenis_produk": 1,
        "sumber_pasokan": "Pengepul"
    },
    "amount": {
        "jumlah": 10000
    }
}

# Flatten
{"source_table": "produksi_susu", "action": "DELETE", "identifier": {"tgl_produksi": "2023-12-23", "id_unit_peternak": 1, "id_jenis_produk": 1, "id_lokasi": 27696, "sumber_pasokan": "Pengepul"}, "amount": {"jumlah": 10000}}

##### UPDATE #####
# Indented
{
    "source_table": "produksi_susu",
    "action": "UPDATE",
    "identifier": {
        "tgl_produksi": "2023-12-23",
        "id_unit_peternak": 1,
        "id_lokasi": 27696,
        "id_jenis_produk": 1,
        "sumber_pasokan": "Pengepul"
    },
    "amount": {
        "jumlah": 8000,
        "prev_jumlah": 10000
    }
}

# Flatten
{"source_table": "produksi_susu", "action": "UPDATE", "identifier": {"tgl_produksi": "2023-12-23", "id_lokasi": 27696, "id_unit_peternak": 1, "id_jenis_produk": 1, "sumber_pasokan": "Pengepul"}, "amount": {"jumlah": 8000, "prev_jumlah": 10000}}

"""
# Event
class IdentifierFactProduksi(pydantic.BaseModel):
    tgl_produksi: date
    id_lokasi: int
    id_unit_peternak: int
    id_jenis_produk: int
    sumber_pasokan: str

class AmountFactProduksi(pydantic.BaseModel):
    jumlah: int

class AmountUpdateFactProduksi(pydantic.BaseModel):
    jumlah: int
    prev_jumlah: int

class EventFactProduksi(pydantic.BaseModel):
    source_table: str
    action: str
    identifier: IdentifierFactProduksi
    amount: Union[AmountUpdateFactProduksi, AmountFactProduksi]

# Table
class TableFactProduksi(pydantic.BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    id_jenis_produk: int
    id_sumber_pasokan: int
    jumlah_produksi: int


"""
Schemas - Distribusi
JSON Sample:

##### CREATE #####
# Indented
{
    "source_table": "distribusi_susu",
    "action": "CREATE",
    "identifier": {
        "tgl_distribusi": "2023-12-23",
        "id_unit_peternak": 1,
        "id_lokasi": 27696,
        "id_mitra_bisnis": 1,
        "id_jenis_produk": 1
    },
    "amount": {
        "jumlah": 10000,
        "harga_berlaku": 2000
    }
}

# Flatten
{"source_table": "distribusi_susu", "action": "CREATE", "identifier": {"tgl_distribusi": "2023-12-23", "id_unit_peternak": 1, "id_lokasi": 27696, "id_mitra_bisnis": 1, "id_jenis_produk": 1}, "amount": {"jumlah": 10000, "harga_berlaku": 2000}}

##### DELETE #####
# Indented
{
    "source_table": "distribusi_susu",
    "action": "DELETE",
    "identifier": {
        "tgl_distribusi": "2023-12-23",
        "id_unit_peternak": 1,
        "id_lokasi": 27696,
        "id_mitra_bisnis": 1,
        "id_jenis_produk": 1
    },
    "amount": {
        "jumlah": 10000,
        "harga_berlaku": 2000
    }
}

# Flatten
{"source_table": "distribusi_susu", "action": "DELETE", "identifier": {"tgl_distribusi": "2023-12-23", "id_unit_peternak": 1, "id_lokasi": 27696, "id_mitra_bisnis": 1, "id_jenis_produk": 1}, "amount": {"jumlah": 10000, "harga_berlaku": 2000}}

##### UPDATE #####
# Indented
{
    "source_table": "distribusi_susu",
    "action": "UPDATE",
    "identifier": {
        "tgl_distribusi": "2023-12-23",
        "id_unit_peternak": 1,
        "id_lokasi": 27696,
        "id_mitra_bisnis": 1,
        "id_jenis_produk": 1
    },
    "amount": {
        "jumlah": 8000,
        "harga_berlaku": 2500,
        "prev_jumlah": 10000,
        "prev_harga_berlaku": 2000
    }
}

# Flatten
{"source_table": "distribusi_susu", "action": "UPDATE", "identifier": {"tgl_distribusi": "2023-12-23", "id_unit_peternak": 1, "id_lokasi": 27696, "id_mitra_bisnis": 1, "id_jenis_produk": 1}, "amount": {"jumlah": 8000, "harga_berlaku": 2500, "prev_jumlah": 10000, "prev_harga_berlaku": 2000}}

"""

# Event
class IdentifierFactDistribusi(pydantic.BaseModel):
    tgl_distribusi: date
    id_unit_peternak: int
    id_lokasi: int
    id_mitra_bisnis: int
    id_jenis_produk: int

class AmountFactDistribusi(pydantic.BaseModel):
    jumlah: int
    harga_berlaku: int

class AmountUpdateFactDistribusi(pydantic.BaseModel):
    jumlah: int
    harga_berlaku: int
    prev_jumlah: int
    prev_harga_berlaku: int

class EventFactDistribusi(pydantic.BaseModel):
    source_table: str
    action: str
    identifier: IdentifierFactDistribusi
    amount: Union[AmountUpdateFactDistribusi, AmountFactDistribusi]

# Table
class TableFactDistribusi(pydantic.BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    id_mitra_bisnis: int
    id_jenis_produk: int
    jumlah_distribusi: int
    jumlah_penjualan: int


"""
Table - Populasi
Source:
    - history_populasi
"""
class TableFactPopulasi(pydantic.BaseModel):
    id_waktu: int
    id_lokasi: int
    id_unit_peternak: int
    jenis_kelamin: str
    tipe_ternak: str
    tipe_usia: str
    jumlah_lahir: int = 0
    jumlah_mati: int = 0
    jumlah_masuk: int = 0
    jumlah_keluar: int = 0
    jumlah: int
    flag_delete: bool


"""
Schemas - Populasi - History Populasi
JSON Sample:

##### CREATE #####
# Indented
{
    "source_table": "history_populasi",
    "action": "CREATE",
    "identifier": {
        "tgl_pencatatan": "2023-12-23",
        "id_peternak": 2
    },
    "amount": {
        "jml_pedaging_jantan": 10,
        "jml_pedaging_betina": 10,
        "jml_pedaging_anakan_jantan": 10,
        "jml_pedaging_anakan_betina": 10,
        "jml_perah_jantan": 10,
        "jml_perah_betina": 10,
        "jml_perah_anakan_jantan": 10,
        "jml_perah_anakan_betina": 10
    }
}

# Flatten
{"source_table": "history_populasi", "action": "CREATE", "identifier": {"tgl_pencatatan": "2024-02-08", "id_unit_peternak": 2, "id_lokasi": 27696}, "amount": {"jml_pedaging_jantan": 8, "jml_pedaging_betina": 8, "jml_pedaging_anakan_jantan": 8, "jml_pedaging_anakan_betina": 20, "jml_perah_jantan": 20, "jml_perah_betina": 8, "jml_perah_anakan_jantan": 8, "jml_perah_anakan_betina": 8}}

##### DELETE #####
# Indented
{
    "source_table": "history_populasi",
    "action": "DELETE",
    "identifier": {
        "tgl_pencatatan": "2023-12-23",
        "id_peternak": 2
    },
    "amount": {
        "jml_pedaging_jantan": 10,
        "jml_pedaging_betina": 10,
        "jml_pedaging_anakan_jantan": 10,
        "jml_pedaging_anakan_betina": 10,
        "jml_perah_jantan": 10,
        "jml_perah_betina": 10,
        "jml_perah_anakan_jantan": 10,
        "jml_perah_anakan_betina": 10
    }
}

# Flatten
{"source_table": "history_populasi", "action": "DELETE", "identifier": {"tgl_pencatatan": "2023-12-23", "id_unit_ternak": 2}, "amount": {"jml_pedaging_jantan": 10, "jml_pedaging_betina": 10, "jml_pedaging_anakan_jantan": 10, "jml_pedaging_anakan_betina": 10, "jml_perah_jantan": 10, "jml_perah_betina": 10, "jml_perah_anakan_jantan": 10, "jml_perah_anakan_betina": 10}}

##### UPDATE #####
# Indented
{
    "source_table": "history_populasi",
    "action": "DELETE",
    "identifier": {
        "tgl_pencatatan": "2023-12-23",
        "id_peternak": 2
    },
    "amount": {
        "jml_pedaging_jantan": 10,
        "jml_pedaging_betina": 10,
        "jml_pedaging_anakan_jantan": 10,
        "jml_pedaging_anakan_betina": 10,
        "jml_perah_jantan": 10,
        "jml_perah_betina": 10,
        "jml_perah_anakan_jantan": 10,
        "jml_perah_anakan_betina": 10,
        "prev_jml_pedaging_jantan": 10,
        "prev_jml_pedaging_betina": 10,
        "prev_jml_pedaging_anakan_jantan": 10,
        "prev_jml_pedaging_anakan_betina": 10,
        "prev_jml_perah_jantan": 10,
        "prev_jml_perah_betina": 10,
        "prev_jml_perah_anakan_jantan": 10,
        "prev_jml_perah_anakan_betina": 10
    }
}

# Flatten
{"source_table": "history_populasi", "action": "DELETE", "identifier": {"tgl_pencatatan": "2023-12-23", "id_peternak": 2}, "amount": {"jml_pedaging_jantan": 10, "jml_pedaging_betina": 10, "jml_pedaging_anakan_jantan": 10, "jml_pedaging_anakan_betina": 10, "jml_perah_jantan": 10, "jml_perah_betina": 10, "jml_perah_anakan_jantan": 10, "jml_perah_anakan_betina": 10, "prev_jml_pedaging_jantan": 10, "prev_jml_pedaging_betina": 10, "prev_jml_pedaging_anakan_jantan": 10, "prev_jml_pedaging_anakan_betina": 10, "prev_jml_perah_jantan": 10, "prev_jml_perah_betina": 10, "prev_jml_perah_anakan_jantan": 10, "prev_jml_perah_anakan_betina": 10}}

"""

class IdentifierFactPopulasi(pydantic.BaseModel):
    tgl_pencatatan: date
    id_unit_peternak: int
    id_lokasi: int

class AmountFactPopulasi(pydantic.BaseModel):
    jml_pedaging_jantan: int
    jml_pedaging_betina: int
    jml_pedaging_anakan_jantan: int
    jml_pedaging_anakan_betina: int
    jml_perah_jantan: int
    jml_perah_betina: int
    jml_perah_anakan_jantan: int
    jml_perah_anakan_betina: int

class AmountUpdateFactPopulasi(AmountFactPopulasi):
    prev_jml_pedaging_jantan: int
    prev_jml_pedaging_betina: int
    prev_jml_pedaging_anakan_jantan: int
    prev_jml_pedaging_anakan_betina: int
    prev_jml_perah_jantan: int
    prev_jml_perah_betina: int
    prev_jml_perah_anakan_jantan: int
    prev_jml_perah_anakan_betina: int

class EventFactPopulasi(pydantic.BaseModel):
    source_table: str
    action: str
    identifier: IdentifierFactPopulasi
    amount: Union[AmountUpdateFactPopulasi, AmountFactPopulasi]