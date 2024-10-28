-- public.history_populasi_cdc source

CREATE OR REPLACE VIEW public.history_populasi_cdc
AS WITH cte_raw_parse AS (
         SELECT log_cdc_prc.operation,
            log_cdc_prc."timestamp",
            (log_cdc_prc.old_data ->> 'id'::text)::bigint AS old_id,
            (log_cdc_prc.old_data ->> 'tgl_pencatatan'::text)::date AS old_tgl_pencatatan,
            (log_cdc_prc.old_data ->> 'id_peternak'::text)::bigint AS old_id_peternak,
            (log_cdc_prc.old_data ->> 'jml_pedaging_jantan'::text)::integer AS old_jml_pedaging_jantan,
            (log_cdc_prc.old_data ->> 'jml_pedaging_betina'::text)::integer AS old_jml_pedaging_betina,
            (log_cdc_prc.old_data ->> 'jml_pedaging_anakan_jantan'::text)::integer AS old_jml_pedaging_anakan_jantan,
            (log_cdc_prc.old_data ->> 'jml_pedaging_anakan_betina'::text)::integer AS old_jml_pedaging_anakan_betina,
            (log_cdc_prc.old_data ->> 'jml_perah_jantan'::text)::integer AS old_jml_perah_jantan,
            (log_cdc_prc.old_data ->> 'jml_perah_betina'::text)::integer AS old_jml_perah_betina,
            (log_cdc_prc.old_data ->> 'jml_perah_anakan_jantan'::text)::integer AS old_jml_perah_anakan_jantan,
            (log_cdc_prc.old_data ->> 'jml_perah_anakan_betina'::text)::integer AS old_jml_perah_anakan_betina,
            (log_cdc_prc.new_data ->> 'id'::text)::bigint AS new_id,
            (log_cdc_prc.new_data ->> 'tgl_pencatatan'::text)::date AS new_tgl_pencatatan,
            (log_cdc_prc.new_data ->> 'id_peternak'::text)::bigint AS new_id_peternak,
            (log_cdc_prc.new_data ->> 'jml_pedaging_jantan'::text)::integer AS new_jml_pedaging_jantan,
            (log_cdc_prc.new_data ->> 'jml_pedaging_betina'::text)::integer AS new_jml_pedaging_betina,
            (log_cdc_prc.new_data ->> 'jml_pedaging_anakan_jantan'::text)::integer AS new_jml_pedaging_anakan_jantan,
            (log_cdc_prc.new_data ->> 'jml_pedaging_anakan_betina'::text)::integer AS new_jml_pedaging_anakan_betina,
            (log_cdc_prc.new_data ->> 'jml_perah_jantan'::text)::integer AS new_jml_perah_jantan,
            (log_cdc_prc.new_data ->> 'jml_perah_betina'::text)::integer AS new_jml_perah_betina,
            (log_cdc_prc.new_data ->> 'jml_perah_anakan_jantan'::text)::integer AS new_jml_perah_anakan_jantan,
            (log_cdc_prc.new_data ->> 'jml_perah_anakan_betina'::text)::integer AS new_jml_perah_anakan_betina
           FROM log_cdc_prc
          WHERE log_cdc_prc.table_name::text = 'history_populasi'::text AND log_cdc_prc.is_processed IS FALSE
        ), cte_op_insert AS (
         SELECT cte_raw_parse.new_id AS id,
            cte_raw_parse.new_tgl_pencatatan AS tgl_pencatatan,
            cte_raw_parse.new_id_peternak AS id_peternak,
            cte_raw_parse.new_jml_pedaging_jantan AS jml_pedaging_jantan,
            cte_raw_parse.new_jml_pedaging_betina AS jml_pedaging_betina,
            cte_raw_parse.new_jml_pedaging_anakan_jantan AS jml_pedaging_anakan_jantan,
            cte_raw_parse.new_jml_pedaging_anakan_betina AS jml_pedaging_anakan_betina,
            cte_raw_parse.new_jml_perah_jantan AS jml_perah_jantan,
            cte_raw_parse.new_jml_perah_betina AS jml_perah_betina,
            cte_raw_parse.new_jml_perah_anakan_jantan AS jml_perah_anakan_jantan,
            cte_raw_parse.new_jml_perah_anakan_betina AS jml_perah_anakan_betina
           FROM cte_raw_parse
          WHERE cte_raw_parse.operation::text = 'INSERT'::text
        ), cte_op_update_flg AS (
         SELECT cte_raw_parse.operation,
            cte_raw_parse."timestamp",
            cte_raw_parse.old_id,
            cte_raw_parse.old_tgl_pencatatan,
            cte_raw_parse.old_id_peternak,
            cte_raw_parse.old_jml_pedaging_jantan,
            cte_raw_parse.old_jml_pedaging_betina,
            cte_raw_parse.old_jml_pedaging_anakan_jantan,
            cte_raw_parse.old_jml_pedaging_anakan_betina,
            cte_raw_parse.old_jml_perah_jantan,
            cte_raw_parse.old_jml_perah_betina,
            cte_raw_parse.old_jml_perah_anakan_jantan,
            cte_raw_parse.old_jml_perah_anakan_betina,
            cte_raw_parse.new_id,
            cte_raw_parse.new_tgl_pencatatan,
            cte_raw_parse.new_id_peternak,
            cte_raw_parse.new_jml_pedaging_jantan,
            cte_raw_parse.new_jml_pedaging_betina,
            cte_raw_parse.new_jml_pedaging_anakan_jantan,
            cte_raw_parse.new_jml_pedaging_anakan_betina,
            cte_raw_parse.new_jml_perah_jantan,
            cte_raw_parse.new_jml_perah_betina,
            cte_raw_parse.new_jml_perah_anakan_jantan,
            cte_raw_parse.new_jml_perah_anakan_betina,
            row_number() OVER (PARTITION BY cte_raw_parse.new_id ORDER BY cte_raw_parse."timestamp") AS flag_asc,
            row_number() OVER (PARTITION BY cte_raw_parse.new_id ORDER BY cte_raw_parse."timestamp" DESC) AS flag_dsc
           FROM cte_raw_parse
          WHERE cte_raw_parse.operation::text = 'UPDATE'::text
        ), cte_op_update AS (
         SELECT nw.id,
            nw.tgl_pencatatan,
            nw.id_peternak,
            nw.jml_pedaging_jantan - ol.jml_pedaging_jantan AS jml_pedaging_jantan,
            nw.jml_pedaging_betina - ol.jml_pedaging_betina AS jml_pedaging_betina,
            nw.jml_pedaging_anakan_jantan - ol.jml_pedaging_anakan_jantan AS jml_pedaging_anakan_jantan,
            nw.jml_pedaging_anakan_betina - ol.jml_pedaging_anakan_betina AS jml_pedaging_anakan_betina,
            nw.jml_perah_jantan - ol.jml_perah_jantan AS jml_perah_jantan,
            nw.jml_perah_betina - ol.jml_perah_betina AS jml_perah_betina,
            nw.jml_perah_anakan_jantan - ol.jml_perah_anakan_jantan AS jml_perah_anakan_jantan,
            nw.jml_perah_anakan_betina - ol.jml_perah_anakan_betina AS jml_perah_anakan_betina
           FROM ( SELECT cte_op_update_flg.old_id AS id,
                    cte_op_update_flg.old_tgl_pencatatan AS tgl_pencatatan,
                    cte_op_update_flg.old_id_peternak AS id_peternak,
                    cte_op_update_flg.old_jml_pedaging_jantan AS jml_pedaging_jantan,
                    cte_op_update_flg.old_jml_pedaging_betina AS jml_pedaging_betina,
                    cte_op_update_flg.old_jml_pedaging_anakan_jantan AS jml_pedaging_anakan_jantan,
                    cte_op_update_flg.old_jml_pedaging_anakan_betina AS jml_pedaging_anakan_betina,
                    cte_op_update_flg.old_jml_perah_jantan AS jml_perah_jantan,
                    cte_op_update_flg.old_jml_perah_betina AS jml_perah_betina,
                    cte_op_update_flg.old_jml_perah_anakan_jantan AS jml_perah_anakan_jantan,
                    cte_op_update_flg.old_jml_perah_anakan_betina AS jml_perah_anakan_betina
                   FROM cte_op_update_flg
                  WHERE cte_op_update_flg.flag_asc = 1) ol
             JOIN ( SELECT cte_op_update_flg.new_id AS id,
                    cte_op_update_flg.new_tgl_pencatatan AS tgl_pencatatan,
                    cte_op_update_flg.new_id_peternak AS id_peternak,
                    cte_op_update_flg.new_jml_pedaging_jantan AS jml_pedaging_jantan,
                    cte_op_update_flg.new_jml_pedaging_betina AS jml_pedaging_betina,
                    cte_op_update_flg.new_jml_pedaging_anakan_jantan AS jml_pedaging_anakan_jantan,
                    cte_op_update_flg.new_jml_pedaging_anakan_betina AS jml_pedaging_anakan_betina,
                    cte_op_update_flg.new_jml_perah_jantan AS jml_perah_jantan,
                    cte_op_update_flg.new_jml_perah_betina AS jml_perah_betina,
                    cte_op_update_flg.new_jml_perah_anakan_jantan AS jml_perah_anakan_jantan,
                    cte_op_update_flg.new_jml_perah_anakan_betina AS jml_perah_anakan_betina
                   FROM cte_op_update_flg
                  WHERE cte_op_update_flg.flag_dsc = 1) nw ON ol.id = nw.id
        ), cte_op_delete AS (
         SELECT cte_raw_parse.old_id AS id,
            cte_raw_parse.old_tgl_pencatatan AS tgl_pencatatan,
            cte_raw_parse.old_id_peternak AS id_peternak,
            '-1'::integer * cte_raw_parse.old_jml_pedaging_jantan AS jml_pedaging_jantan,
            '-1'::integer * cte_raw_parse.old_jml_pedaging_betina AS jml_pedaging_betina,
            '-1'::integer * cte_raw_parse.old_jml_pedaging_anakan_jantan AS jml_pedaging_anakan_jantan,
            '-1'::integer * cte_raw_parse.old_jml_pedaging_anakan_betina AS jml_pedaging_anakan_betina,
            '-1'::integer * cte_raw_parse.old_jml_perah_jantan AS jml_perah_jantan,
            '-1'::integer * cte_raw_parse.old_jml_perah_betina AS jml_perah_betina,
            '-1'::integer * cte_raw_parse.old_jml_perah_anakan_jantan AS jml_perah_anakan_jantan,
            '-1'::integer * cte_raw_parse.old_jml_perah_anakan_betina AS jml_perah_anakan_betina
           FROM cte_raw_parse
          WHERE cte_raw_parse.operation::text = 'DELETE'::text
        ), cte_cdc_view AS (
         SELECT COALESCE(i.id, u.id, d.id) AS id,
            COALESCE(i.tgl_pencatatan, u.tgl_pencatatan, d.tgl_pencatatan) AS tgl_pencatatan,
            COALESCE(i.id_peternak, u.id_peternak, d.id_peternak) AS id_peternak,
            COALESCE(i.jml_pedaging_jantan, 0) + COALESCE(u.jml_pedaging_jantan, 0) + COALESCE(d.jml_pedaging_jantan, 0) AS jml_pedaging_jantan,
            COALESCE(i.jml_pedaging_betina, 0) + COALESCE(u.jml_pedaging_betina, 0) + COALESCE(d.jml_pedaging_betina, 0) AS jml_pedaging_betina,
            COALESCE(i.jml_pedaging_anakan_jantan, 0) + COALESCE(u.jml_pedaging_anakan_jantan, 0) + COALESCE(d.jml_pedaging_anakan_jantan, 0) AS jml_pedaging_anakan_jantan,
            COALESCE(i.jml_pedaging_anakan_betina, 0) + COALESCE(u.jml_pedaging_anakan_betina, 0) + COALESCE(d.jml_pedaging_anakan_betina, 0) AS jml_pedaging_anakan_betina,
            COALESCE(i.jml_perah_jantan, 0) + COALESCE(u.jml_perah_jantan, 0) + COALESCE(d.jml_perah_jantan, 0) AS jml_perah_jantan,
            COALESCE(i.jml_perah_betina, 0) + COALESCE(u.jml_perah_betina, 0) + COALESCE(d.jml_perah_betina, 0) AS jml_perah_betina,
            COALESCE(i.jml_perah_anakan_jantan, 0) + COALESCE(u.jml_perah_anakan_jantan, 0) + COALESCE(d.jml_perah_anakan_jantan, 0) AS jml_perah_anakan_jantan,
            COALESCE(i.jml_perah_anakan_betina, 0) + COALESCE(u.jml_perah_anakan_betina, 0) + COALESCE(d.jml_perah_anakan_betina, 0) AS jml_perah_anakan_betina
           FROM cte_op_insert i
             FULL JOIN cte_op_update u ON i.id = u.id
             FULL JOIN cte_op_delete d ON i.id = d.id AND u.id = d.id
        )
 SELECT cte_cdc_view.id,
    cte_cdc_view.tgl_pencatatan,
    cte_cdc_view.id_peternak,
    cte_cdc_view.jml_pedaging_jantan,
    cte_cdc_view.jml_pedaging_betina,
    cte_cdc_view.jml_pedaging_anakan_jantan,
    cte_cdc_view.jml_pedaging_anakan_betina,
    cte_cdc_view.jml_perah_jantan,
    cte_cdc_view.jml_perah_betina,
    cte_cdc_view.jml_perah_anakan_jantan,
    cte_cdc_view.jml_perah_anakan_betina
   FROM cte_cdc_view;