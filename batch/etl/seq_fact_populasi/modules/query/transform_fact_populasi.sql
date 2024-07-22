WITH cte_history_kelahiran_kematian AS (
  SELECT
    hkk.tgl_pencatatan,
    hkk.id_peternak AS id_peternakan,
    jml.jenis_kelamin,
    jml.tipe_ternak,
    jml.tipe_usia,
    SUM(jml.jumlah_lahir) AS jumlah_lahir,
    SUM(jml.jumlah_mati) AS jumlah_mati,
    NULL::INT8 AS jumlah_masuk,
    NULL::INT8 AS jumlah_keluar,
    NULL::INT8 AS jumlah
  FROM history_kelahiran_kematian_cdc AS hkk
  CROSS JOIN LATERAL (
    VALUES
      ('Jantan', 'Pedaging', 'Dewasa', NULL::INT8, hkk.jml_mati_pedaging_jantan),
      ('Jantan', 'Pedaging', 'Anakan', hkk.jml_lahir_pedaging_jantan, hkk.jml_mati_pedaging_anakan_jantan),
      ('Jantan', 'Perah', 'Dewasa', NULL::INT8, hkk.jml_mati_perah_jantan),
      ('Jantan', 'Perah', 'Anakan', hkk.jml_lahir_perah_jantan, hkk.jml_mati_perah_anakan_jantan),
      ('Betina', 'Pedaging','Dewasa', NULL::INT8, hkk.jml_mati_pedaging_betina),
      ('Betina', 'Pedaging','Anakan', hkk.jml_lahir_pedaging_betina, hkk.jml_mati_pedaging_anakan_betina),
      ('Betina', 'Perah','Dewasa', NULL::INT8, hkk.jml_mati_perah_betina),
      ('Betina', 'Perah','Anakan', hkk.jml_lahir_perah_betina, hkk.jml_mati_perah_anakan_betina)
  ) AS jml (jenis_kelamin, tipe_ternak, tipe_usia, jumlah_lahir, jumlah_mati)
  GROUP BY 1, 2, 3, 4, 5
),
cte_pencatatan_ternak_masuk AS (
  SELECT
    ptm.tgl_pencatatan,
    ptm.id_peternak AS id_peternakan,
    jml.jenis_kelamin,
    jml.tipe_ternak,
    jml.tipe_usia,
    NULL::INT8 AS jumlah_lahir,
    NULL::INT8 AS jumlah_mati,
    SUM(jml.jumlah_masuk) AS jumlah_masuk,
    NULL::INT8 AS jumlah_keluar,
    NULL::INT8 AS jumlah
  FROM pencatatan_ternak_masuk_cdc AS ptm
  CROSS JOIN LATERAL (
    VALUES 
      ('Jantan', 'Pedaging', 'Dewasa', ptm.jml_pedaging_jantan),
      ('Jantan', 'Pedaging', 'Anakan', ptm.jml_pedaging_anakan_jantan),
      ('Jantan', 'Perah', 'Dewasa', ptm.jml_perah_jantan),
      ('Jantan', 'Perah', 'Anakan', ptm.jml_perah_anakan_jantan),
      ('Betina', 'Pedaging', 'Dewasa', ptm.jml_pedaging_betina),
      ('Betina', 'Pedaging', 'Anakan', ptm.jml_pedaging_anakan_betina),
      ('Betina', 'Perah', 'Dewasa', ptm.jml_perah_betina),
      ('Betina', 'Perah', 'Anakan', ptm.jml_perah_anakan_betina)
  ) AS jml (jenis_kelamin, tipe_ternak, tipe_usia, jumlah_masuk)
  GROUP BY 1, 2, 3, 4, 5
),
cte_pencatatan_ternak_keluar AS (
  SELECT
    ptk.tgl_pencatatan,
    ptk.id_peternak AS id_peternakan,
    jml.jenis_kelamin,
    jml.tipe_ternak,
    jml.tipe_usia,
    NULL::INT8 AS jumlah_lahir,
    NULL::INT8 AS jumlah_mati,
    NULL::INT8 AS jumlah_masuk,
    SUM(jml.jumlah_keluar) AS jumlah_keluar,
    NULL::INT8 AS jumlah
  FROM pencatatan_ternak_keluar_cdc AS ptk
  CROSS JOIN LATERAL (
    VALUES 
      ('Jantan', 'Pedaging', 'Dewasa', ptk.jml_pedaging_jantan),
      ('Jantan', 'Pedaging', 'Anakan', ptk.jml_pedaging_anakan_jantan),
      ('Jantan', 'Perah', 'Dewasa', ptk.jml_perah_jantan),
      ('Jantan', 'Perah', 'Anakan', ptk.jml_perah_anakan_jantan),
      ('Betina', 'Pedaging', 'Dewasa', ptk.jml_pedaging_betina),
      ('Betina', 'Pedaging', 'Anakan', ptk.jml_pedaging_anakan_betina),
      ('Betina', 'Perah', 'Dewasa', ptk.jml_perah_betina),
      ('Betina', 'Perah', 'Anakan', ptk.jml_perah_anakan_betina)
  ) AS jml (jenis_kelamin, tipe_ternak, tipe_usia, jumlah_keluar)
  GROUP BY 1, 2, 3, 4, 5
),
cte_history_populasi AS (
  SELECT
    hp.tgl_pencatatan,
    hp.id_peternak AS id_peternakan,
    jml.jenis_kelamin,
    jml.tipe_ternak,
    jml.tipe_usia,
    NULL::INT8 AS jumlah_lahir,
    NULL::INT8 AS jumlah_mati,
    NULL::INT8 AS jumlah_masuk,
    NULL::INT8 AS jumlah_keluar,
    SUM(jml.jumlah) AS jumlah
  FROM history_populasi_cdc AS hp
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
),
cte_summary AS (
  SELECT
    tgl_pencatatan,
    id_peternakan,
    jenis_kelamin,
    tipe_ternak,
    tipe_usia,
    SUM(jumlah_lahir) AS jumlah_lahir,
    SUM(jumlah_mati) AS jumlah_mati,
    SUM(jumlah_masuk) AS jumlah_masuk,
    SUM(jumlah_keluar) AS jumlah_keluar,
    SUM(jumlah) AS jumlah
  FROM (
    SELECT * FROM cte_history_kelahiran_kematian
    UNION ALL
    SELECT * FROM cte_pencatatan_ternak_masuk
    UNION ALL
    SELECT * FROM cte_pencatatan_ternak_keluar
    UNION ALL
    SELECT * FROM cte_history_populasi
  ) AS ud
  GROUP BY 1, 2, 3, 4, 5
)
SELECT * FROM cte_summary
ORDER BY 1, 2, 3, 4, 5;
