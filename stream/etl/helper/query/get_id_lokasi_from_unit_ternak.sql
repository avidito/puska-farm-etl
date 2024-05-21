SELECT
  l.provinsi,
  l.kabupaten_kota,
  l.kecamatan
FROM dim_lokasi AS l
JOIN dim_unit_peternakan AS p
  ON l.id = p.id_lokasi
WHERE p.id = :id_unit_ternak