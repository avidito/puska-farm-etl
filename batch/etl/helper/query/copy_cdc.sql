INSERT INTO log_cdc_prc
SELECT
  *,
  FALSE AS is_processed
FROM log_cdc
WHERE table_name = :table_name;
