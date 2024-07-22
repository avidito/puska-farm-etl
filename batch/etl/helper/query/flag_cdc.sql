UPDATE log_cdc_prc
SET
  is_processed = TRUE
WHERE table_name = :table_name
