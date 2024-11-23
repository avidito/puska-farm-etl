WITH cte_fltr AS (
  SELECT
    l.provinsi,
    l.kabupaten_kota  
  FROM dim_unit_peternakan AS up
  JOIN dim_lokasi AS l
    ON up.id_lokasi = l.id
  WHERE up.id = :id_unit_peternakan
),
cte_provinsi AS (
  SELECT l.id, l.provinsi
  FROM dim_lokasi AS l
  JOIN cte_fltr AS fltr
    ON l.provinsi = fltr.provinsi    
  WHERE l.kabupaten_kota IS NULL
),
cte_kabupaten_kota AS (
  SELECT l.id, l.kabupaten_kota
  FROM dim_lokasi AS l
  JOIN cte_fltr AS fltr
    ON l.kabupaten_kota = fltr.kabupaten_kota
  WHERE l.kecamatan IS NULL
)
SELECT
  p.id AS id_provinsi,
  p.provinsi AS label_provinsi,
  kk.id AS id_kabupaten_kota,
  kk.kabupaten_kota AS label_kabupaten_kota
FROM cte_kabupaten_kota AS kk
CROSS JOIN cte_provinsi AS p;
