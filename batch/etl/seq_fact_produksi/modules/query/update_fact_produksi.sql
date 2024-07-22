INSERT INTO fact_produksi (
  id_waktu,
  id_lokasi,
  id_unit_peternakan,
  id_jenis_produk,
  id_sumber_pasokan,
  jumlah_produksi,
  created_dt,
  modified_dt
)
SELECT
  w.id AS id_waktu,
  l.id AS id_lokasi,
  t.id_unit_peternakan,
  t.id_jenis_produk,
  sp.id AS id_sumber_pasokan,
  t.jumlah_produksi,
  TIMEZONE('Asia/Jakarta', NOW()) AS created_dt,
  TIMEZONE('Asia/Jakarta', NOW()) AS modified_dt
FROM tmp_fact_produksi AS t
LEFT JOIN dim_waktu AS w
  ON t.tgl_produksi = w.tanggal
LEFT JOIN dim_unit_peternakan AS up
  ON t.id_unit_peternakan = up.id
LEFT JOIN dim_lokasi AS l
  ON up.id_lokasi = l.id
LEFT JOIN dim_sumber_pasokan AS sp
  ON t.sumber_pasokan = sp.nama_sumber_pasokan
ON CONFLICT (id_waktu, id_lokasi, id_unit_peternakan, id_jenis_produk, id_sumber_pasokan)
DO UPDATE SET
  jumlah_produksi = fact_produksi.jumlah_produksi + EXCLUDED.jumlah_produksi,
  modified_dt = TIMEZONE('Asia/Jakarta', NOW());
