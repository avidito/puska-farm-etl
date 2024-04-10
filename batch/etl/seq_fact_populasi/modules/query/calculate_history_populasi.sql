WITH cte_filter AS (
  SELECT
    DATE(:start_date) AS start_date,
    DATE(:end_date) AS end_date
),
cte_history_populasi AS (
  SELECT
    hp.tgl_pencatatan,
    hp.id_peternak AS id_peternakan,
    jml.jenis_kelamin,
    jml.tipe_ternak,
    jml.tipe_usia,
    SUM(jml.jumlah) AS jumlah
  FROM history_populasi AS hp
  JOIN cte_filter AS fltr
    ON hp.tgl_pencatatan BETWEEN fltr.start_date AND fltr.end_date
  CROSS JOIN LATERAL (
    VALUES 
      ('Jantan', 'Pedaging', 'Dewasa', hp.jml_pedaging_jantan),
      ('Jantan', 'Pedaging', 'Anakan', hp.jml_pedaging_anakan_jantan),
      ('Jantan', 'Perah', 'Dewasa', hp.jml_perah_jantan),
      ('Jantan', 'Perah', 'Anakan', hp.jml_perah_anakan_jantan),
      ('Betina', 'Pedaging', 'Dewasa', hp.jml_pedaging_betina),
      ('Betina', 'Pedaging', 'Anakan', hp.jml_pedaging_anakan_betina),
      ('Betina', 'Perah', 'Dewasa', hp.jml_perah_betina),
      ('Betina', 'Perah', 'Anakan', hp.jml_perah_anakan_betina)
  ) AS jml (jenis_kelamin, tipe_ternak, tipe_usia, jumlah)
  GROUP BY 1, 2, 3, 4, 5
)
SELECT * FROM cte_history_populasi;