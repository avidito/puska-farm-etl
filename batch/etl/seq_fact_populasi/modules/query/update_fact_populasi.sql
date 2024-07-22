INSERT INTO fact_populasi (
  id_waktu,
  id_lokasi,
  id_peternakan,
  jenis_kelamin,
  tipe_ternak,
  tipe_usia,
  jumlah_lahir,
  jumlah_mati,
  jumlah_masuk,
  jumlah_keluar,
  jumlah,
  created_dt,
  modified_dt
)
SELECT
  w.id AS id_waktu,
  l.id AS id_lokasi,
  t.id_peternakan,
  t.jenis_kelamin,
  t.tipe_ternak,
  t.tipe_usia,
  t.jumlah_lahir,
  t.jumlah_mati,
  t.jumlah_masuk,
  t.jumlah_keluar,
  t.jumlah,
  TIMEZONE('Asia/Jakarta', NOW()) AS created_dt,
  TIMEZONE('Asia/Jakarta', NOW()) AS modified_dt
FROM tmp_fact_populasi AS t
LEFT JOIN dim_waktu AS w
  ON t.tgl_pencatatan = w.tanggal
LEFT JOIN (
  SELECT
    p.id,
    up.id_lokasi
  FROM dim_peternakan AS p
  LEFT JOIN dim_unit_peternakan AS up
    ON p.id_unit_peternakan = up.id
  LIMIT 1
) AS p_up
  ON t.id_peternakan = p_up.id
LEFT JOIN dim_lokasi AS l
  ON p_up.id_lokasi = l.id
ON CONFLICT (id_waktu, id_lokasi, id_peternakan, jenis_kelamin, tipe_ternak, tipe_usia)
DO UPDATE SET
  jumlah_lahir = EXCLUDED.jumlah_lahir,
  jumlah_mati = EXCLUDED.jumlah_mati,
  jumlah_masuk = EXCLUDED.jumlah_masuk,
  jumlah_keluar = EXCLUDED.jumlah_keluar,
  jumlah = EXCLUDED.jumlah,
  modified_dt = TIMEZONE('Asia/Jakarta', NOW());
