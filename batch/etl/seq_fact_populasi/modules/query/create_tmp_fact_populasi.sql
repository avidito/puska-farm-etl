CREATE TEMPORARY TABLE tmp_fact_populasi (
  tgl_pencatatan DATE,
  id_peternakan INT8,
  jenis_kelamin VARCHAR(10),
  tipe_ternak VARCHAR(15),
  tipe_usia VARCHAR(15),
  jumlah_lahir INT8,
  jumlah_mati INT8,
  jumlah_masuk INT8,
  jumlah_keluar INT8,
  jumlah INT8,
  created_dt TIMESTAMP,
  modified_dt TIMESTAMP
);
