SELECT sp.id
FROM dim_sumber_pasokan AS sp
WHERE sp.nama_sumber_pasokan = :nama_sumber_pasokan;