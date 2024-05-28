SELECT
  l.provinsi,
  l.kabupaten_kota,
  l.kecamatan
FROM dim_peternakan AS pt
JOIN dim_unit_peternak AS p
  ON pt.id_unit_peternak = p.id
JOIN dim_lokasi AS l
  ON p.id_lokasi = l.id
WHERE pt.id = :id_peternak
LIMIT 1