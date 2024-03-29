SELECT
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
  jumlah
FROM fact_populasi_stream
WHERE id_waktu = :id_waktu
  AND id_lokasi = :id_lokasi
  AND id_peternakan = :id_peternakan
  AND jenis_kelamin = :jenis_kelamin
  AND tipe_ternak = :tipe_ternak
  AND tipe_usia = :tipe_usia;