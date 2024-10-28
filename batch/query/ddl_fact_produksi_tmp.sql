DROP TABLE fact_produksi_tmp;
CREATE TABLE fact_produksi_tmp (
  tgl_produksi DATE,
  id_unit_peternakan INT8,
  id_jenis_produk INT8,
  sumber_pasokan VARCHAR,
  jumlah_produksi NUMERIC,
  created_dt TIMESTAMP,
  modified_dt TIMESTAMP
);
