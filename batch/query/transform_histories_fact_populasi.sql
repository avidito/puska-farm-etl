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
  999 AS id_peternakan,
  '' AS jenis_kelamin,
  '' AS tipe_ternak,
  '' AS tipe_usia,
  NULL::INT AS jumlah_lahir,
  NULL::INT AS jumlah_mati,
  NULL::INT AS jumlah_masuk,
  NULL::INT AS jumlah_keluar,
  h.jumlah,
  CURRENT_TIMESTAMP AS created_dt,
  CURRENT_TIMESTAMP AS modified_dt
FROM histories_fact_populasi AS h
LEFT JOIN (
  SELECT id, provinsi
  FROM dim_lokasi
  WHERE kabupaten_kota IS NULL
) AS l
  ON h.provinsi = l.provinsi
LEFT JOIN (
  SELECT id, tahun
  FROM dim_waktu
  WHERE bulan IS NULL
    AND hari IS NULL
) AS w
  ON h.tahun = w.tahun
WHERE kabupaten_kota = '';

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
  999 AS id_peternakan,
  '' AS jenis_kelamin,
  '' AS tipe_ternak,
  '' AS tipe_usia,
  NULL::INT AS jumlah_lahir,
  NULL::INT AS jumlah_mati,
  NULL::INT AS jumlah_masuk,
  NULL::INT AS jumlah_keluar,
  h.jumlah,
  CURRENT_TIMESTAMP AS created_dt,
  CURRENT_TIMESTAMP AS modified_dt
FROM histories_fact_populasi AS h
LEFT JOIN (
  SELECT id, provinsi, kabupaten_kota
  FROM dim_lokasi
  WHERE kabupaten_kota IS NOT NULL
    AND kecamatan IS NULL
) AS l
  ON h.provinsi = l.provinsi
  AND h.kabupaten_kota = l.kabupaten_kota
LEFT JOIN (
  SELECT id, tahun
  FROM dim_waktu
  WHERE bulan IS NULL
    AND hari IS NULL
) AS w
  ON h.tahun = w.tahun
WHERE h.kabupaten_kota <> ''
  AND h.kecamatan = '';

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
  999 AS id_peternakan,
  '' AS jenis_kelamin,
  '' AS tipe_ternak,
  '' AS tipe_usia,
  NULL::INT AS jumlah_lahir,
  NULL::INT AS jumlah_mati,
  NULL::INT AS jumlah_masuk,
  NULL::INT AS jumlah_keluar,
  h.jumlah,
  CURRENT_TIMESTAMP AS created_dt,
  CURRENT_TIMESTAMP AS modified_dt
FROM histories_fact_populasi AS h
LEFT JOIN (
  SELECT id, provinsi, kabupaten_kota, kecamatan
  FROM dim_lokasi
  WHERE kecamatan IS NOT NULL
) AS l
  ON h.provinsi = l.provinsi
  AND h.kabupaten_kota = l.kabupaten_kota
  AND TRIM(h.kecamatan) = TRIM(l.kecamatan)
LEFT JOIN (
  SELECT id, tahun
  FROM dim_waktu
  WHERE bulan IS NULL
    AND hari IS NULL
) AS w
  ON h.tahun = w.tahun
WHERE h.kecamatan <> ''
  AND l.id IS NOT NULL;