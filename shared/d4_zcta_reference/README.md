# Shared D4 area reference (provisional)

`nyc_zcta_reference.csv` lists the 221 NYC ZIP Code Tabulation Areas from D4
(NYC OTI *ZIP Code Tabulation Areas*, NYC Open Data `35j5-n34v`), one row per code:
`Zip Code, Land Area, Internal Latitude, Internal Longitude` (no geometry).

- Retrieved 2026-11-19 from the portal API. D4 rows were last updated 2026-04-10, so the codes
  are the same as the team's 2026-10-27 D4 snapshot.
- MD5 `b774cc47390c76b7754a93bca820ad3f`.
- Both `d1_dog_licensing` and `d5_311` read it as-is (column "Zip Code" is auto-detected).
- Check: D1's 2026 extract matches 96,019 rows across 196 codes, the proposal's preliminary count.

Member 3 owns D4. When the cleaned reference is ready, point every module at that one file
instead. On HDFS keep a single file in `pawprint/raw/d4_zcta/`: the loaders read every file in
that directory, so two different reference files there would be merged.
