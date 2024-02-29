SELECT id AS id_sumber_pasokan
FROM dim_sumber_pasokan
WHERE TRUE
  AND nama_sumber_pasokan = :sumber_pasokan;