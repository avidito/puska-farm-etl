INSERT INTO fact_distribusi (
  id_waktu,
  id_lokasi,
  id_unit_peternakan,
  id_mitra_bisnis,
  id_jenis_produk,
  jumlah_distribusi,
  harga_minimum,
  harga_maximum,
  harga_rata_rata,
  jumlah_penjualan,
  created_dt,
  modified_dt
)
SELECT
  w.id AS id_waktu,
  l.id AS id_lokasi,
  t.id_unit_peternakan,
  t.id_mitra_bisnis,
  t.id_jenis_produk,
  t.jumlah_distribusi,
  t.harga_minimum,
  t.harga_maximum,
  t.harga_rata_rata,
  t.jumlah_penjualan,
  TIMEZONE('Asia/Jakarta', NOW()) AS created_dt,
  TIMEZONE('Asia/Jakarta', NOW()) AS modified_dt
FROM tmp_fact_distribusi AS t
LEFT JOIN dim_waktu AS w
  ON t.tgl_distribusi = w.tanggal
LEFT JOIN dim_unit_peternakan AS up
  ON t.id_unit_peternakan = up.id
LEFT JOIN dim_lokasi AS l
  ON up.id_lokasi = l.id
ON CONFLICT (id_waktu, id_lokasi, id_unit_peternakan, id_mitra_bisnis, id_jenis_produk)
DO UPDATE SET
  jumlah_distribusi = fact_distribusi.jumlah_distribusi + EXCLUDED.jumlah_distribusi,
  harga_minimum = (CASE WHEN EXCLUDED.harga_minimum = 0 THEN 0 ELSE LEAST(fact_distribusi.harga_minimum, EXCLUDED.harga_minimum) END),
  harga_maximum = (CASE WHEN EXCLUDED.harga_maximum = 0 THEN 0 ELSE GREATEST(fact_distribusi.harga_maximum, EXCLUDED.harga_maximum) END),
  harga_rata_rata = COALESCE(((fact_distribusi.jumlah_distribusi * fact_distribusi.harga_rata_rata) + (EXCLUDED.jumlah_distribusi * EXCLUDED.harga_rata_rata)) / NULLIF(fact_distribusi.jumlah_distribusi + EXCLUDED.jumlah_distribusi, 0), 0),
  jumlah_penjualan = fact_distribusi.jumlah_penjualan + EXCLUDED.jumlah_penjualan,
  modified_dt = TIMEZONE('Asia/Jakarta', NOW());
