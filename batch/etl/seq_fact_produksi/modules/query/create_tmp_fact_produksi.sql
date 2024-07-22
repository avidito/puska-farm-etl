CREATE TEMPORARY TABLE tmp_fact_produksi (
  tgl_produksi DATE,
  id_unit_peternakan INT8,
  id_jenis_produk INT8,
  sumber_pasokan VARCHAR(20),
  jumlah_produksi NUMERIC,
  created_dt TIMESTAMP,
  modified_dt TIMESTAMP
);