DROP TABLE fact_distribusi_tmp;
CREATE TABLE fact_distribusi_tmp (
  tgl_distribusi DATE,
  id_unit_peternakan INT8,
  id_mitra_bisnis INT8,
  id_jenis_produk INT8,
  jumlah_distribusi NUMERIC,
  harga_minimum INT8,
  harga_maximum INT8,
  harga_rata_rata NUMERIC,
  jumlah_penjualan NUMERIC,
  created_dt TIMESTAMP,
  modified_dt TIMESTAMP
);