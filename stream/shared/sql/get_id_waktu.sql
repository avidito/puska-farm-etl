SELECT id AS id_waktu
FROM dim_waktu
WHERE TRUE
  AND tanggal = :tanggal;
  