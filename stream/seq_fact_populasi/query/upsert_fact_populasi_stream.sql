INSERT INTO fact_populasi_stream (
  id_waktu,
  id_lokasi,
  id_unit_peternak,
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
VALUES (
  :id_waktu,
  :id_lokasi,
  :id_unit_peternak,
  :jenis_kelamin,
  :tipe_ternak,
  :tipe_usia,
  :jumlah_lahir,
  :jumlah_mati,
  :jumlah_masuk,
  :jumlah_keluar,
  :jumlah,
  CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta',
  CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'
)
ON CONFLICT (id_waktu, id_lokasi, id_unit_peternak, jenis_kelamin, tipe_ternak, tipe_usia)
DO UPDATE SET
  jumlah_lahir = EXCLUDED.jumlah_lahir,
  jumlah_mati = EXCLUDED.jumlah_mati,
  jumlah_masuk = EXCLUDED.jumlah_masuk,
  jumlah_keluar = EXCLUDED.jumlah_keluar,
  jumlah = EXCLUDED.jumlah,
  modified_dt = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta';
