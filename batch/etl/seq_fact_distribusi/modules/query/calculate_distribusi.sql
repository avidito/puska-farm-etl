WITH cte_filter AS (
  SELECT
    DATE(:start_date) AS start_date,
    DATE(:end_date) AS end_date
),
cte_distribusi_susu AS (
  SELECT
    s.tgl_distribusi,
    s.id_unit_ternak AS id_unit_peternakan,
    s.id_mitra_bisnis,
    s.id_jenis_produk,
    ROUND(SUM(s.jumlah), 3) AS jumlah_distribusi,
    MIN(s.harga_berlaku) AS harga_minimum,
    MAX(s.harga_berlaku) AS harga_maximum,
    ROUND(AVG(s.harga_berlaku), 3) AS harga_rata_rata,
    ROUND(SUM(s.jumlah * s.harga_berlaku), 3) AS jumlah_penjualan
  FROM distribusi_susu AS s
  JOIN cte_filter AS fltr
    ON s.tgl_distribusi BETWEEN fltr.start_date AND fltr.end_date
  GROUP BY 1, 2, 3, 4
),
cte_distribusi_ternak AS (
  SELECT
    t.tgl_distribusi,
    t.id_unit_ternak AS id_unit_peternakan,
    t.id_mitra_bisnis,
    t.id_jenis_produk,
    ROUND(SUM(t.jumlah), 3) AS jumlah_distribusi,
    MIN(t.harga_berlaku) AS harga_minimum,
    MAX(t.harga_berlaku) AS harga_maximum,
    ROUND(AVG(t.harga_berlaku), 3) AS harga_rata_rata,
    ROUND(SUM(t.jumlah * t.harga_berlaku), 3) AS jumlah_penjualan
  FROM distribusi_ternak AS t
  JOIN cte_filter AS fltr
    ON t.tgl_distribusi BETWEEN fltr.start_date AND fltr.end_date
  GROUP BY 1, 2, 3, 4
),
cte_summary AS (
  SELECT * FROM cte_distribusi_susu
  UNION ALL
  SELECT * FROM cte_distribusi_ternak
)
SELECT * FROM cte_summary
ORDER BY 1, 2, 3, 4;
