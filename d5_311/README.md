# D5 – 311 Dog Off Leash: MapReduce profiling and cleaning (Member 4, hpw2708)

Java Hadoop MapReduce jobs for the 2025 NYC 311 *Animal in a Park* extract (proposal §5, "D5 311
service requests"). The jobs profile the raw extract, clean and validate it, filter it to *Dog Off
Leash* reports, match ZIPs to the D4 ZCTA reference, and aggregate reports by area code.

## Build

```bash
cd d5_311
mvn package            # builds target/d5-311.jar and runs the tests (Java 8+; Hadoop 3.3.6 is "provided")
```

The tests include an end-to-end run of the whole pipeline with Hadoop's LocalJobRunner on
`src/test/resources/sample_311.csv`. Build with JDK 8, 11 or 17. Hadoop 3.3's local runner does not start on JDK 23+.

## Run on the cluster

```bash
scripts/run_d5_hdfs.sh 311_animal_in_park_2025.csv nyc_zcta.csv            # uploads + runs everything
# or directly:
hadoop jar target/d5-311.jar edu.nyu.pawprint.d5.D5Pipeline -D d5.overwrite=true \
    /user/$USER/pawprint/raw/d5_311  /user/$USER/pawprint/raw/d4_zcta  /user/$USER/pawprint/d5_311
```

Each job also runs on its own: `ProfileJob <in> <out>`, `DistinctJob <freq> <out>`,
`CleanJob <in> <zipref> <out>`, `AggregateJob <cleaned> <zipref> <out>`.

| Option | Meaning |
|---|---|
| `-D d5.overwrite=true` | delete existing output directories first |
| `-D d5.year=2025` | year a `created_date` must fall in |
| `-D d5.zip.ref.column=<name>` | ZCTA code column in the D4 file (auto-detected otherwise: `zcta`, `zcta5ce20`, `modzcta`, `zipcode`, …) |
| `-D d5.columns=a,b,c` | column order, if the extract has no header (normally read from the header) |

The input header can use either Socrata API names (`unique_key`) or portal names (`Unique Key`).
The D4 reference can be the raw OTI CSV, including WKT geometry that spans lines, or Member 3's
cleaned code list.

## Pipeline and outputs (`<out>` = pipeline base path)

| Stage | Output | Contents |
|---|---|---|
| 1 ProfileJob | `profile/raw/stats` | `column  metric  value`: ROWS, MISSING, NON_MISSING, TYPE_{INTEGER,DECIMAL,TIMESTAMP,STRING}, MIN/MAX_NUMBER, MIN/MAX_TIMESTAMP, MIN/MAX_LENGTH; `_record` holds INPUT_LINES, DATA_ROWS, HEADER_LINES, BLANK_LINES, MALFORMED_CSV, FIELD_COUNT_MISMATCH |
| | `profile/raw/freq` | `column  value  count` categorical frequencies (`<MISSING>` for missing) |
| 2 DistinctJob | `profile/distinct/distinct` | `column  distinct_non_missing  missing_rows  values_repeated  rows_beyond_first  max_frequency` |
| | `profile/distinct/duplicates` | candidate duplicate `unique_key`s and duplicate full rows (`_row_md5`) |
| 3 CleanJob | `clean/cleaned` | retained Dog Off Leash reports (schema below) |
| | `clean/rejected` | `reasons  file:offset  raw_line` |
| | `clean/filtered` | valid rows outside scope (`OTHER_DESCRIPTOR`, `OTHER_COMPLAINT_TYPE`) |
| | `clean/unmatched` | audit copy of retained reports whose ZIP is UNMATCHED / INVALID / MISSING |
| | `clean/reconciliation.txt` | all counters and the checks below |
| 4 ProfileJob | `profile/cleaned/*` | the same profile, run on the cleaned output |
| 5 AggregateJob | `aggregate/by_zip` | `zip  reports  closed  other_status  first_date  last_date  boroughs`, one row per D4 code |
| | `aggregate/reconciliation.txt` | aggregation checks |

Cleaned schema (tab-separated, `\N` = NULL): `unique_key, created_ts, created_date, complaint_type,
descriptor, incident_zip, incident_zip_raw, zip_status, borough, status, quality_flags`.
Hive DDL for these tables is in [hive/d5_311_tables.hql](hive/d5_311_tables.hql).

## Cleaning rules

**Rejected** (written to `clean/rejected`, all reasons listed):
malformed CSV · wrong field count · missing or non-positive-integer `unique_key` · missing or
unparseable `created_date` (the API `2025-01-05T08:30:00.000`, `2025-01-05 08:30:00` and portal
`01/05/2025 08:30:00 AM` formats are accepted; impossible dates such as Feb 30 are rejected) ·
date outside 2025 · missing `complaint_type` or `descriptor` · duplicate `unique_key`. For a
duplicate key, the earliest report is kept and the others are labeled `DUPLICATE_ROW` (an
identical line) or `DUPLICATE_KEY_CONFLICT` (same ID, different content).

**Filtered** (`clean/filtered`): complaint type other than *Animal in a Park* · descriptor other
than *Dog Off Leash*. Matching ignores case, spacing and hyphens, so "dog off-leash" counts.

**Kept and flagged** (`quality_flags`). These problems do not reject a report, so citywide
counts stay complete:
- ZIP: `ZIP_MISSING`, `ZIP_INVALID`, `ZIP_NOT_IN_REFERENCE`, `ZIP_PLUS4_TRUNCATED`. ZIP+4 values
  are cut to 5 digits; `00000` counts as invalid. `zip_status` records MATCHED/UNMATCHED/INVALID/MISSING
  against D4.
- Borough: upper-cased canonical name; `BOROUGH_UNSPECIFIED`, `BOROUGH_UNRECOGNIZED`.
- Status: canonical label (`Closed`, `Open`, `In Progress`, …); `STATUS_MISSING`, `STATUS_UNRECOGNIZED`.

Only MATCHED reports are aggregated by ZIP. Every D4 code gets a row, and a `0` means no matched
reports, which is different from missing data.

## Reconciliation

CleanJob fails (exit 1) unless both checks hold:

```
input_lines = blank + header + rejected + filtered + retained
retained    = MATCHED + UNMATCHED + INVALID + MISSING
```

AggregateJob checks `matched_records = reports_aggregated` and
`zips_with_reports + zero_rows = reference_codes`.

Against the real extract, the proposal's preliminary counts give the expected order of magnitude:
5,416 rows, 3,006 Dog Off Leash, 3,004 ZIP matches across 146 codes.

## Caveats

- A complaint is a reported event, not evidence of unmet demand or causation.
- Records are read one line at a time (TextInputFormat). A quoted field with an embedded newline
  shows up as `MALFORMED_CSV` and is counted, not silently dropped. The selected 311 columns are
  not expected to contain newlines.
- An exact ZIP code match to a ZCTA approximates the geography (proposal §6).
