SELECT l.id
FROM dim_lokasi AS l
WHERE l.provinsi = :provinsi
  AND l.kabupaten_kota = :kabupaten_kota
  AND l.kecamatan = :kecamatan