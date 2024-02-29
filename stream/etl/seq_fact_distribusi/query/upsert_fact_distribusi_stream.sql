INSERT INTO fact_distribusi_stream (
  id_waktu,
  id_lokasi,
  id_unit_peternak,
  id_mitra_bisnis,
  id_jenis_produk,
  jumlah_distribusi,
  harga_rata_rata,
  jumlah_penjualan,
  created_dt,
  modified_dt
)
VALUES (
  :id_waktu,
  :id_lokasi,
  :id_unit_peternak,
  :id_mitra_bisnis,
  :id_jenis_produk,
  :jumlah_distribusi,
  (:jumlah_penjualan / :jumlah_distribusi),
  :jumlah_penjualan,
  CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta',
  CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta'
)
ON CONFLICT (id_waktu, id_lokasi, id_unit_peternak, id_mitra_bisnis, id_jenis_produk)
DO UPDATE SET
  jumlah_distribusi = EXCLUDED.jumlah_distribusi + fact_distribusi_stream.jumlah_distribusi,
  harga_rata_rata = (EXCLUDED.jumlah_penjualan + fact_distribusi_stream.jumlah_penjualan) / (EXCLUDED.jumlah_distribusi + fact_distribusi_stream.jumlah_distribusi),
  jumlah_penjualan = EXCLUDED.jumlah_penjualan + fact_distribusi_stream.jumlah_penjualan,
  modified_dt = CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Jakarta';