# D1 – NYC Dog Licensing: MapReduce profiling and cleaning (Member 1, yy6389)

Java Hadoop MapReduce jobs for the NYC Dog Licensing Dataset (NYC DOHMH, NYC Open Data
`nu7n-tubp`; proposal §5, "D1 Licensing"). The jobs profile the raw export, clean and validate
every license-period record, match owner ZIPs to the D4 ZCTA reference, and count license
periods per area code for the 2026 extract. The module follows the same layout and conventions
as [`d5_311`](../d5_311) so the area-level outputs join the same way in Hive.

## Build

```bash
cd d1_dog_licensing
mvn package                 # builds target/d1-dog-licensing.jar and runs the tests (Java 8+; Hadoop 3.3.6 is "provided")
scripts/build_javac.sh      # or, on the cluster without Maven: javac against `yarn classpath`
```

The tests include an end-to-end run of the whole pipeline with Hadoop's LocalJobRunner on
`src/test/resources/sample_dogs.csv`, a fixture that exercises every reject and flag rule.

## Run on the cluster

```bash
scripts/run_d1_hdfs.sh NYC_Dog_Licensing_Dataset_20261030.csv nyc_zcta_reference.csv   # uploads + runs everything
# or directly:
hadoop jar target/d1-dog-licensing.jar edu.nyu.pawprint.d1.D1Pipeline -D d1.overwrite=true \
    /user/$USER/pawprint/raw/d1_dogs  /user/$USER/pawprint/raw/d4_zcta  /user/$USER/pawprint/d1_dogs
```

Each job also runs on its own (the jar has no default main class, so name the class):
`ProfileJob <in> <out>`, `DistinctJob <freq> <out>`, `CleanJob <in> <zipref> <out>`,
`AggregateJob <cleaned> <zipref> <out>`.

| Option | Meaning |
|---|---|
| `-D d1.overwrite=true` | delete existing output directories first |
| `-D d1.extract.year=2026` | extract year used for the ZIP-level counts |
| `-D d1.zip.ref.column=<name>` | ZCTA code column in the D4 file (auto-detected otherwise, as in D5) |
| `-D d1.columns=a,b,c` | column order, if the export has no header (normally read from the header) |

The input header can use the portal names (`AnimalName`, …, `Extract Year`) or the Socrata API
names (`animalname`, `animalbirth`, …, `extract_year`). The D4 reference can be the raw OTI CSV
(WKT geometry may span lines), Member 3's cleaned code list, or the attribute-only extract used
for the checkpoint (`nyc_zcta_reference.csv`, 221 codes, MD5 `b774cc47390c76b7754a93bca820ad3f`).

## Pipeline and outputs (`<out>` = pipeline base path)

| Stage | Output | Contents |
|---|---|---|
| 1 ProfileJob | `profile/raw/stats` | `column  metric  value`: ROWS, MISSING, NON_MISSING, TYPE_{INTEGER,DECIMAL,DATE,STRING}, MIN/MAX_NUMBER, MIN/MAX_DATE, MIN/MAX_LENGTH; `_record` holds INPUT_LINES, DATA_ROWS, HEADER_LINES, BLANK_LINES, MALFORMED_CSV, FIELD_COUNT_MISMATCH; `_license_days` min/max; `_extract_year_YYYY` rows and issue/expiry date range |
| | `profile/raw/freq` | `column  value  count` value frequencies (`<MISSING>` for missing), plus `_zip_shape`, `_license_days` buckets, `_row_md5`, `_candidate_dog_key` |
| 2 DistinctJob | `profile/distinct/distinct` | `column  distinct_non_missing  missing_rows  values_repeated  rows_beyond_first  max_frequency` |
| | `profile/distinct/duplicates` | identical rows (`_row_md5`, prefixed with the extract year) and repeated candidate dog keys |
| 3 CleanJob | `clean/cleaned` | retained license periods, all extract years (schema below) |
| | `clean/rejected` | `reasons  file:offset  raw_line` |
| | `clean/unmatched` | audit copy of retained records whose ZIP is UNMATCHED / INVALID / MISSING |
| | `clean/reconciliation.txt` | all counters and the checks below |
| 4 ProfileJob | `profile/cleaned/*` | the same profile, run on the cleaned output |
| 5 AggregateJob | `aggregate/by_zip` | `zip  license_periods  distinct_license_rows  identical_repeat_rows`, one row per D4 code, 2026 extract |
| | `aggregate/reconciliation.txt` | aggregation checks |

Cleaned schema (tab-separated, `\N` = NULL): `extract_year, license_issued_date,
license_expired_date, license_days, owner_zip, owner_zip_raw, zip_status, animal_birth_year,
animal_gender, breed_name, animal_name, quality_flags`. Hive DDL is in
[hive/d1_dog_licensing_tables.hql](hive/d1_dog_licensing_tables.hql).

## Cleaning rules

**Rejected** (written to `clean/rejected`, all reasons listed): malformed CSV · wrong field
count · missing or invalid `Extract Year` · missing or invalid issue date (impossible dates such
as Feb 30 are invalid) · issue date after the extract year · expiry before issue.

**Kept and flagged** (`quality_flags`), so citywide counts stay complete, as in D5:
- ZIP: `ZIP_MISSING`, `ZIP_INVALID`, `ZIP_NOT_IN_REFERENCE`, `ZIP_PLUS4_TRUNCATED`,
  `ZIP_LEADING_ZERO_RESTORED` (four-digit values such as 7302 become 07302). `zip_status`
  records MATCHED/UNMATCHED/INVALID/MISSING against D4.
- License: `EXPIRY_MISSING`, `EXPIRY_INVALID`, `LICENSE_OVER_5_YEARS` (DOHMH licenses run one
  to five years; flagged past five years plus 31 days).
- Animal: `BIRTH_YEAR_MISSING`, `BIRTH_YEAR_INVALID` (not a four-digit year within 30 years
  before issue), `GENDER_MISSING`, `GENDER_UNRECOGNIZED`, `BREED_UNKNOWN` (blank, Unknown,
  Not Provided), `NAME_MISSING` (blank, UNKNOWN, NAME NOT PROVIDED, or a missing token such as
  NONE / NA). Placeholder values are written as `\N`.

There is no published dog or license ID, so nothing is deduplicated: the unit is the license
period, and `by_zip` reports counts with and without collapsing identical rows. Only MATCHED
records of the selected extract are aggregated. Every D4 code gets a row, and a `0` means no
matched license periods, which is different from missing data.

## Reconciliation

CleanJob fails (exit 1) unless both checks hold:

```
input_lines = blank + header + rejected + retained
retained    = MATCHED + UNMATCHED + INVALID + MISSING = sum over extract years
```

AggregateJob checks `matched_records = periods_aggregated` and
`zips_with_records + zero_rows = reference_codes`.

Results on the 2026-10-30 snapshot (819,323 rows, 64,518,360 bytes, MD5
`9601d48d2487f3de628ed38f5b7771b2`) with the 221-code reference:

| Check | Value |
|---|---|
| input lines = header + rejected + retained | 819,324 = 1 + 0 + 819,323 |
| zip_status, all years | MATCHED 815,707 · UNMATCHED 3,597 · INVALID 10 · MISSING 9 |
| 2026 extract | 96,459 = MATCHED 96,019 + UNMATCHED 438 + INVALID 2 |
| by_zip (2026) | 196 codes with records + 25 zero rows = 221; 96,019 periods, 95,266 distinct rows |

These reproduce the proposal's preliminary counts (96,019 matched across 196 codes; 440 to review).

## Caveats

- `Extract Year` is the year the data was pulled, not the license year: the 2026 extract holds
  licenses active during 2025 (issued 2017-12-05 to 2025-12-31, expiring 2025-01-01 or later).
  Extracts overlap, so never add counts across extract years.
- License periods measure registration activity, not unique dogs; compliance varies by area.
- An exact owner-ZIP match to a ZCTA approximates the geography (proposal §6).
- Records are read one line at a time (TextInputFormat). A quoted field with an embedded
  newline shows up as `MALFORMED_CSV` and is counted, not silently dropped.
