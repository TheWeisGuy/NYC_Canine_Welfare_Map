package edu.nyu.pawprint.d1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.junit.BeforeClass;
import org.junit.Test;

/** Runs the whole D1 pipeline on the sample fixtures with Hadoop's LocalJobRunner. */
public class PipelineLocalTest {

  private static File out;

  @BeforeClass
  public static void runPipeline() throws Exception {
    out = new File(Files.createTempDirectory("d1-pipeline").toFile(), "out");
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");
    conf.setInt("mapreduce.job.reduces", 2); // exercise zero-fill partitioning
    int rc = ToolRunner.run(conf, new D1Pipeline(), new String[] {
        resource("sample_dogs.csv"), resource("sample_zcta.csv"), out.getAbsolutePath()});
    assertEquals(0, rc);
  }

  private static String resource(String name) {
    return new File(PipelineLocalTest.class.getClassLoader().getResource(name).getFile()).getAbsolutePath();
  }

  /** All lines of all part files under OUT/sub, sorted. */
  private static List<String> lines(String sub) throws IOException {
    List<String> all = new ArrayList<String>();
    File[] files = new File(out, sub).listFiles();
    if (files != null) {
      for (File f : files) {
        if (f.getName().startsWith("part-")) {
          all.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8));
        }
      }
    }
    Collections.sort(all);
    return all;
  }

  private static String text(String path) throws IOException {
    return new String(Files.readAllBytes(new File(out, path).toPath()), StandardCharsets.UTF_8);
  }

  private static long countReason(List<String> lines, String reason) {
    long n = 0;
    for (String l : lines) {
      if (l.startsWith(reason + "\t")) {
        n++;
      }
    }
    return n;
  }

  @Test
  public void reconciliationPasses() throws IOException {
    String rec = text("clean/reconciliation.txt");
    assertTrue(rec, rec.contains("input_lines(20) = blank(1) + header(1) + rejected(6) + retained(12)\tPASS"));
    assertTrue(rec, rec.contains("retained(12) = sum(zip_status)(12) = sum(extract_year)(12)\tPASS"));
    assertTrue(rec, rec.contains("D1_ZIP_STATUS\tMATCHED\t8"));
    String agg = text("aggregate/reconciliation.txt");
    assertTrue(agg, agg.contains("matched_records(7) = periods_aggregated(7)\tPASS"));
    assertTrue(agg, agg.contains("zips_with_records(3) + zero_rows(1) = reference_codes(4)\tPASS"));
  }

  @Test
  public void cleanedRecords() throws IOException {
    List<String> cleaned = lines("clean/cleaned");
    assertEquals(12, cleaned.size());
    assertTrue(cleaned.toString(), cleaned.contains(
        "2026\t2018-11-23\t2027-06-09\t3120\t07305\t7305\tMATCHED\t2006\tM\t\\N\tTINY\t"
            + "LICENSE_OVER_5_YEARS;ZIP_LEADING_ZERO_RESTORED;BREED_UNKNOWN"));
    assertTrue(cleaned.toString(), cleaned.contains(
        "2026\t2021-03-28\t2026-02-13\t1783\t10001\t10001-1234\tMATCHED\t2016\tM\t\\N\t\\N\t"
            + "ZIP_PLUS4_TRUNCATED;BREED_UNKNOWN;NAME_MISSING"));
    assertTrue(cleaned.toString(), cleaned.contains(
        "2022\t2019-05-01\t\\N\t\\N\t11215\t11215\tMATCHED\t2015\t\\N\tBeagle\tBELLA\tEXPIRY_MISSING;GENDER_MISSING"));
    assertTrue(cleaned.toString(), cleaned.contains(
        "2026\t2021-01-11\t2026-02-12\t1858\t\\N\t923\tINVALID\t2018\tM\tPoodle Crossbreed\tTIGER\tLICENSE_OVER_5_YEARS;ZIP_INVALID"));
    // unmatched audit copy: LUNA x2 (UNMATCHED), TIGER (INVALID), MAX (MISSING)
    assertEquals(4, lines("clean/unmatched").size());
  }

  @Test
  public void rejected() throws IOException {
    List<String> rejected = lines("clean/rejected");
    assertEquals(6, rejected.size());
    assertEquals(1, countReason(rejected, "EXPIRY_BEFORE_ISSUE"));
    assertEquals(1, countReason(rejected, "INVALID_ISSUE_DATE"));
    assertEquals(1, countReason(rejected, "ISSUE_DATE_AFTER_EXTRACT_YEAR"));
    assertEquals(1, countReason(rejected, "INVALID_EXTRACT_YEAR"));
    assertEquals(1, countReason(rejected, "MALFORMED_CSV"));
    assertEquals(1, countReason(rejected, "FIELD_COUNT_MISMATCH"));
  }

  @Test
  public void aggregateByZip() throws IOException {
    List<String> byZip = lines("aggregate/by_zip");
    assertEquals(4, byZip.size());
    assertTrue(byZip.toString(), byZip.contains("10001\t4\t3\t1"));   // COCO repeated once
    assertTrue(byZip.toString(), byZip.contains("11215\t2\t2\t0"));
    assertTrue(byZip.toString(), byZip.contains("07305\t1\t1\t0"));
    assertTrue(byZip.toString(), byZip.contains("10458\t0\t0\t0"));   // explicit zero row
  }

  @Test
  public void profiles() throws IOException {
    List<String> stats = lines("profile/raw/stats");
    assertTrue(stats.toString(), stats.contains("_record\tINPUT_LINES\t20"));
    assertTrue(stats.toString(), stats.contains("_record\tDATA_ROWS\t16"));
    assertTrue(stats.toString(), stats.contains("_record\tMALFORMED_CSV\t1"));
    assertTrue(stats.toString(), stats.contains("zipcode\tMISSING\t1"));
    assertTrue(stats.toString(), stats.contains("licenseissueddate\tMIN_DATE\t2018-11-23"));
    assertTrue(stats.toString(), stats.contains("_extract_year_2026\tROWS\t11"));
    List<String> freq = lines("profile/raw/freq");
    assertTrue(freq.toString(), freq.contains("breedname\tJindo Dog, Korea\t1"));
    assertTrue(freq.toString(), freq.contains("_zip_shape\t9999\t1"));

    List<String> distinct = lines("profile/distinct/distinct");
    // 16 data rows, 14 distinct lines: LUNA and COCO each repeated once.
    assertTrue(distinct.toString(), distinct.contains("_row_md5\t14\t0\t2\t2\t2"));
    List<String> cleanedStats = lines("profile/cleaned/stats");
    assertTrue(cleanedStats.toString(), cleanedStats.contains("extract_year\tROWS\t12"));
    assertTrue(cleanedStats.toString(), cleanedStats.contains("owner_zip\tMISSING\t2"));
  }
}
