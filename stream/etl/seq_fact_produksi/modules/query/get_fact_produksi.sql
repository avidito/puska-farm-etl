SELECT
  id_waktu,
  id_lokasi,
  id_unit_peternak,
  id_jenis_produk,
  id_sumber_pasokan,
  jumlah_produksi
FROM fact_produksi_stream
WHERE id_waktu = :id_waktu
  AND id_lokasi = :id_lokasi
  AND id_unit_peternak = :id_unit_peternak
  AND id_jenis_produk = :id_jenis_produk;