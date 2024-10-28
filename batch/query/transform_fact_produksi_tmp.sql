INSERT INTO fact_produksi
SELECT
  w.id AS id_waktu,
  up.id_lokasi,
  pro.id_unit_peternakan,
  pro.id_jenis_produk,
  sp.id AS id_sumber_pasokan,
  pro.jumlah_produksi,
  pro.created_dt,
  pro.modified_dt
FROM fact_produksi_tmp AS pro
LEFT JOIN dim_waktu AS w
  ON pro.tgl_produksi = w.tanggal
LEFT JOIN dim_unit_peternakan AS up
  ON pro.id_unit_peternakan = up.id
LEFT JOIN dim_sumber_pasokan AS sp
  ON pro.sumber_pasokan = sp.nama_sumber_pasokan;