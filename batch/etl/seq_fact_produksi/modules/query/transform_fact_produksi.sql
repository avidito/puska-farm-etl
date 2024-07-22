WITH cte_produksi_susu AS (
  SELECT
    s.tgl_produksi,
    s.id_unit_ternak AS id_unit_peternakan,
    s.id_jenis_produk,
    s.sumber_pasokan,
    ROUND(SUM(s.jumlah), 3) AS jumlah_produksi
  FROM produksi_susu_cdc AS s
  GROUP BY 1, 2, 3, 4
),
cte_produksi_ternak AS (
  SELECT
    t.tgl_produksi,
    t.id_unit_ternak AS id_unit_peternakan,
    t.id_jenis_produk,
    t.sumber_pasokan,
    ROUND(SUM(t.jumlah), 3) AS jumlah_produksi
  FROM produksi_ternak_cdc AS t
  GROUP BY 1, 2, 3, 4
),
cte_summary AS (
  SELECT
    tgl_produksi,
    id_unit_peternakan,
    id_jenis_produk,
    sumber_pasokan,
    jumlah_produksi
  FROM (
    SELECT * FROM cte_produksi_susu
    UNION ALL
    SELECT * FROM cte_produksi_ternak
  ) AS ud
)
SELECT * FROM cte_summary
ORDER BY 1, 2, 3, 4;
