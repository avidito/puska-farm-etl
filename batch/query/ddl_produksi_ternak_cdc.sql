-- public.produksi_ternak_cdc source

CREATE OR REPLACE VIEW public.produksi_ternak_cdc
AS WITH cte_raw_parse AS (
         SELECT log_cdc_prc.operation,
            log_cdc_prc."timestamp",
            (log_cdc_prc.old_data ->> 'id'::text)::bigint AS old_id,
            (log_cdc_prc.old_data ->> 'tgl_produksi'::text)::date AS old_tgl_produksi,
            (log_cdc_prc.old_data ->> 'id_unit_ternak'::text)::bigint AS old_id_unit_ternak,
            (log_cdc_prc.old_data ->> 'id_jenis_produk'::text)::bigint AS old_id_jenis_produk,
            ((log_cdc_prc.old_data ->> 'sumber_pasokan'::text))::character varying(255) AS old_sumber_pasokan,
            ((log_cdc_prc.old_data ->> 'satuan'::text))::character varying(255) AS old_satuan,
            ((log_cdc_prc.old_data ->> 'jumlah'::text))::numeric(8,2) AS old_jumlah,
            (log_cdc_prc.old_data ->> 'harga_berlaku'::text)::bigint AS old_harga_berlaku,
            (log_cdc_prc.new_data ->> 'id'::text)::bigint AS new_id,
            (log_cdc_prc.new_data ->> 'tgl_produksi'::text)::date AS new_tgl_produksi,
            (log_cdc_prc.new_data ->> 'id_unit_ternak'::text)::bigint AS new_id_unit_ternak,
            (log_cdc_prc.new_data ->> 'id_jenis_produk'::text)::bigint AS new_id_jenis_produk,
            ((log_cdc_prc.new_data ->> 'sumber_pasokan'::text))::character varying(255) AS new_sumber_pasokan,
            ((log_cdc_prc.new_data ->> 'satuan'::text))::character varying(255) AS new_satuan,
            ((log_cdc_prc.new_data ->> 'jumlah'::text))::numeric(8,2) AS new_jumlah,
            (log_cdc_prc.new_data ->> 'harga_berlaku'::text)::bigint AS new_harga_berlaku
           FROM log_cdc_prc
          WHERE log_cdc_prc.table_name::text = 'produksi_ternak'::text AND log_cdc_prc.is_processed IS FALSE
        ), cte_op_insert AS (
         SELECT cte_raw_parse.new_id AS id,
            cte_raw_parse.new_tgl_produksi AS tgl_produksi,
            cte_raw_parse.new_id_unit_ternak AS id_unit_ternak,
            cte_raw_parse.new_id_jenis_produk AS id_jenis_produk,
            cte_raw_parse.new_sumber_pasokan AS sumber_pasokan,
            cte_raw_parse.new_satuan AS satuan,
            cte_raw_parse.new_jumlah AS jumlah,
            cte_raw_parse.new_harga_berlaku AS harga_berlaku
           FROM cte_raw_parse
          WHERE cte_raw_parse.operation::text = 'INSERT'::text
        ), cte_op_update_flag AS (
         SELECT cte_raw_parse.operation,
            cte_raw_parse."timestamp",
            cte_raw_parse.old_id,
            cte_raw_parse.old_tgl_produksi,
            cte_raw_parse.old_id_unit_ternak,
            cte_raw_parse.old_id_jenis_produk,
            cte_raw_parse.old_sumber_pasokan,
            cte_raw_parse.old_satuan,
            cte_raw_parse.old_jumlah,
            cte_raw_parse.old_harga_berlaku,
            cte_raw_parse.new_id,
            cte_raw_parse.new_tgl_produksi,
            cte_raw_parse.new_id_unit_ternak,
            cte_raw_parse.new_id_jenis_produk,
            cte_raw_parse.new_sumber_pasokan,
            cte_raw_parse.new_satuan,
            cte_raw_parse.new_jumlah,
            cte_raw_parse.new_harga_berlaku,
            row_number() OVER (PARTITION BY cte_raw_parse.new_id ORDER BY cte_raw_parse."timestamp") AS flag_asc,
            row_number() OVER (PARTITION BY cte_raw_parse.new_id ORDER BY cte_raw_parse."timestamp" DESC) AS flag_dsc
           FROM cte_raw_parse
          WHERE cte_raw_parse.operation::text = 'UPDATE'::text
        ), cte_op_update AS (
         SELECT new_1.id,
            new_1.tgl_produksi,
            new_1.id_unit_ternak,
            new_1.id_jenis_produk,
            new_1.sumber_pasokan,
            new_1.satuan,
            new_1.jumlah - old_1.jumlah AS jumlah,
            new_1.harga_berlaku - old_1.harga_berlaku AS harga_berlaku
           FROM ( SELECT cte_op_update_flag.old_id AS id,
                    cte_op_update_flag.old_tgl_produksi AS tgl_produksi,
                    cte_op_update_flag.old_id_unit_ternak AS id_unit_ternak,
                    cte_op_update_flag.old_id_jenis_produk AS id_jenis_produk,
                    cte_op_update_flag.old_sumber_pasokan AS sumber_pasokan,
                    cte_op_update_flag.old_satuan AS satuan,
                    cte_op_update_flag.old_jumlah AS jumlah,
                    cte_op_update_flag.old_harga_berlaku AS harga_berlaku
                   FROM cte_op_update_flag
                  WHERE cte_op_update_flag.flag_asc = 1) old_1
             JOIN ( SELECT cte_op_update_flag.new_id AS id,
                    cte_op_update_flag.new_tgl_produksi AS tgl_produksi,
                    cte_op_update_flag.new_id_unit_ternak AS id_unit_ternak,
                    cte_op_update_flag.new_id_jenis_produk AS id_jenis_produk,
                    cte_op_update_flag.new_sumber_pasokan AS sumber_pasokan,
                    cte_op_update_flag.new_satuan AS satuan,
                    cte_op_update_flag.new_jumlah AS jumlah,
                    cte_op_update_flag.new_harga_berlaku AS harga_berlaku
                   FROM cte_op_update_flag
                  WHERE cte_op_update_flag.flag_dsc = 1) new_1 ON old_1.id = new_1.id
        ), cte_op_delete AS (
         SELECT cte_raw_parse.old_id AS id,
            cte_raw_parse.old_tgl_produksi AS tgl_produksi,
            cte_raw_parse.old_id_unit_ternak AS id_unit_ternak,
            cte_raw_parse.old_id_jenis_produk AS id_jenis_produk,
            cte_raw_parse.old_sumber_pasokan AS sumber_pasokan,
            cte_raw_parse.old_satuan AS satuan,
            '-1'::integer::numeric * cte_raw_parse.old_jumlah AS jumlah,
            '-1'::integer * cte_raw_parse.old_harga_berlaku AS harga_berlaku
           FROM cte_raw_parse
          WHERE cte_raw_parse.operation::text = 'DELETE'::text
        ), cte_cdc_view AS (
         SELECT COALESCE(i.id, u.id, d.id) AS id,
            COALESCE(i.tgl_produksi, u.tgl_produksi, d.tgl_produksi) AS tgl_produksi,
            COALESCE(i.id_unit_ternak, u.id_unit_ternak, d.id_unit_ternak) AS id_unit_ternak,
            COALESCE(i.id_jenis_produk, u.id_jenis_produk, d.id_jenis_produk) AS id_jenis_produk,
            COALESCE(i.sumber_pasokan, u.sumber_pasokan, d.sumber_pasokan) AS sumber_pasokan,
            COALESCE(i.satuan, u.satuan, d.satuan) AS satuan,
            COALESCE(i.jumlah, 0::numeric) + COALESCE(u.jumlah, 0::numeric) + COALESCE(d.jumlah, 0::numeric) AS jumlah,
            COALESCE(i.harga_berlaku, 0::bigint) + COALESCE(u.harga_berlaku, 0::bigint) + COALESCE(d.harga_berlaku, 0::bigint) AS harga_berlaku
           FROM cte_op_insert i
             FULL JOIN cte_op_update u ON i.id = u.id
             FULL JOIN cte_op_delete d ON i.id = d.id AND u.id = d.id
        )
 SELECT cte_cdc_view.id,
    cte_cdc_view.tgl_produksi,
    cte_cdc_view.id_unit_ternak,
    cte_cdc_view.id_jenis_produk,
    cte_cdc_view.sumber_pasokan,
    cte_cdc_view.satuan,
    cte_cdc_view.jumlah,
    cte_cdc_view.harga_berlaku
   FROM cte_cdc_view;