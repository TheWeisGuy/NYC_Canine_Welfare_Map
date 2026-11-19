-- External Hive tables over the D5 311 MapReduce outputs.
-- Set the base path to the pipeline output directory, e.g.
--   hive --hivevar d5_base=/user/<netid>/pawprint/d5_311 -f hive/d5_311_tables.hql

CREATE EXTERNAL TABLE IF NOT EXISTS d5_311_dog_off_leash_clean (
  unique_key        BIGINT,
  created_ts        TIMESTAMP,
  created_date      DATE,
  complaint_type    STRING,
  descriptor        STRING,
  incident_zip      STRING,   -- five-digit ZIP, NULL when missing/invalid
  incident_zip_raw  STRING,
  zip_status        STRING,   -- MATCHED | UNMATCHED | INVALID | MISSING (vs. D4 reference)
  borough           STRING,
  status            STRING,
  quality_flags     STRING    -- ';'-separated, empty when none
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:d5_base}/clean/cleaned';

-- One row per D4 area code; 0 = no matched reports (not missing data).
CREATE EXTERNAL TABLE IF NOT EXISTS d5_311_dog_off_leash_by_zip (
  zip                    STRING,
  dog_off_leash_reports  BIGINT,
  closed_reports         BIGINT,
  other_status_reports   BIGINT,
  first_report_date      DATE,
  last_report_date       DATE,
  boroughs               STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:d5_base}/aggregate/by_zip';

-- Audit: rejected and filtered raw lines with their reasons.
CREATE EXTERNAL TABLE IF NOT EXISTS d5_311_rejected (reasons STRING, source STRING, raw_line STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '${hivevar:d5_base}/clean/rejected';

CREATE EXTERNAL TABLE IF NOT EXISTS d5_311_filtered (reason STRING, source STRING, raw_line STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '${hivevar:d5_base}/clean/filtered';
