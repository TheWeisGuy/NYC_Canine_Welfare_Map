-- External Hive tables over the D1 dog licensing MapReduce outputs.
-- Set the base path to the pipeline output directory, e.g.
--   hive --hivevar d1_base=/user/<netid>/pawprint/d1_dogs -f hive/d1_dog_licensing_tables.hql

-- One row per license period (not per dog), all extract years.
CREATE EXTERNAL TABLE IF NOT EXISTS d1_dog_licenses_clean (
  extract_year          INT,
  license_issued_date   DATE,
  license_expired_date  DATE,     -- NULL when missing or unparseable
  license_days          INT,
  owner_zip             STRING,   -- five-digit ZIP, NULL when missing/invalid
  owner_zip_raw         STRING,
  zip_status            STRING,   -- MATCHED | UNMATCHED | INVALID | MISSING (vs. D4 reference)
  animal_birth_year     INT,      -- NULL when missing or implausible
  animal_gender         STRING,   -- M | F, NULL when missing
  breed_name            STRING,   -- NULL for Unknown / Not Provided
  animal_name           STRING,   -- NULL for UNKNOWN / NAME NOT PROVIDED
  quality_flags         STRING    -- ';'-separated, empty when none
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:d1_base}/clean/cleaned';

-- One row per D4 area code for the selected extract (default 2026);
-- 0 = no matched license periods (not missing data).
CREATE EXTERNAL TABLE IF NOT EXISTS d1_dog_license_periods_by_zip (
  zip                     STRING,
  license_periods         BIGINT,   -- recorded-demand measure (proposal section 7)
  distinct_license_rows   BIGINT,   -- identical rows collapsed (sensitivity check)
  identical_repeat_rows   BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '${hivevar:d1_base}/aggregate/by_zip';

-- Audit: rejected raw lines with their reasons.
CREATE EXTERNAL TABLE IF NOT EXISTS d1_dog_licenses_rejected (reasons STRING, source STRING, raw_line STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' STORED AS TEXTFILE
LOCATION '${hivevar:d1_base}/clean/rejected';
