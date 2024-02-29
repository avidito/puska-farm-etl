SELECT id_unit_peternak
FROM dim_peternakan
WHERE TRUE
  AND id = :id_peternak;
