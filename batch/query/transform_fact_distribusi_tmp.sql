INSERT INTO fact_distribusi
SELECT
  w.id AS id_waktu,
  pt.id_lokasi,
  dis.id_unit_peternakan,
  dis.id_mitra_bisnis,
  dis.id_jenis_produk,
  dis.jumlah_distribusi,
  dis.harga_minimum,
  dis.harga_maximum,
  dis.harga_rata_rata,
  dis.jumlah_penjualan,
  dis.created_dt,
  dis.modified_dt
FROM fact_distribusi_tmp AS dis
LEFT JOIN dim_waktu AS w
  ON dis.tgl_distribusi = w.tanggal
LEFT JOIN dim_unit_peternakan AS pt
  ON dis.id_unit_peternakan = pt.id;
