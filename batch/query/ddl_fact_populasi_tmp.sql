DROP TABLE fact_populasi_tmp;
CREATE TABLE fact_populasi_tmp (
  tgl_pencatatan DATE,
  id_peternakan INT8,
  jenis_kelamin VARCHAR(10),
  tipe_ternak VARCHAR(15) NULL,
  tipe_usia VARCHAR(15) NULL,
  jumlah_lahir INT8,
  jumlah_mati INT8,
  jumlah_masuk INT8,
  jumlah_keluar INT8,
  jumlah INT8,
  created_dt TIMESTAMP,
  modified_dt TIMESTAMP,
  PRIMARY KEY(tgl_pencatatan, id_peternakan, jenis_kelamin, tipe_ternak, tipe_usia)
);
