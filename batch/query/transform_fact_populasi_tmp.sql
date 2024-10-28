INSERT INTO fact_populasi
SELECT
  w.id AS id_waktu,
  up.id_lokasi,
  pop.id_peternakan,
  pop.jenis_kelamin,
  pop.tipe_ternak,
  pop.tipe_usia,
  pop.jumlah_lahir,
  pop.jumlah_mati,
  pop.jumlah_masuk,
  pop.jumlah_keluar,
  pop.jumlah,
  pop.created_dt,
  pop.modified_dt
FROM fact_populasi_tmp AS pop
LEFT JOIN dim_waktu AS w
  ON pop.tgl_pencatatan = w.tanggal
LEFT JOIN dim_peternakan AS pt
  ON pop.id_peternakan = pt.id
JOIN dim_unit_peternakan AS up
  ON pt.id_unit_peternakan = up.id;
