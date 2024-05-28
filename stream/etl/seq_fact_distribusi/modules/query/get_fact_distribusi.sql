SELECT
  id_waktu,
  id_lokasi,
  id_unit_peternak,
  id_mitra_bisnis,
  id_jenis_produk,
  jumlah_distribusi,
  harga_minimum,
  harga_maximum,
  harga_rata_rata,
  jumlah_penjualan
FROM fact_distribusi_stream
WHERE id_waktu = :id_waktu
  AND id_lokasi = :id_lokasi
  AND id_unit_peternak = :id_unit_peternak
  AND id_mitra_bisnis = :id_mitra_bisnis
  AND id_jenis_produk = :id_jenis_produk;