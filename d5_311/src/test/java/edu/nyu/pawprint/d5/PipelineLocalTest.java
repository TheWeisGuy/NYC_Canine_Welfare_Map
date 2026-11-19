package edu.nyu.pawprint.d5;

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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/** Runs the whole pipeline on the sample fixtures with Hadoop's LocalJobRunner. */
public class PipelineLocalTest {

  @ClassRule
  public static TemporaryFolder tmp = new TemporaryFolder();

  private static File out;

  @BeforeClass
  public static void runPipeline() throws Exception {
    out = new File(tmp.getRoot(), "out");
    Configuration conf = new Configuration();
    conf.set("mapreduce.framework.name", "local");
    conf.set("fs.defaultFS", "file:///");
    conf.setInt("mapreduce.job.reduces", 2); // exercise zero-fill partitioning
    int rc = ToolRunner.run(conf, new D5Pipeline(), new String[] {
        resource("sample_311.csv"), resource("sample_zcta.csv"), out.getAbsolutePath()});
    assertEquals(0, rc);
  }

  private static String resource(String name) {
    return new File(PipelineLocalTest.class.getClassLoader().getResource(name).getFile()).getAbsolutePath();
  }

  /** All lines of all part files under OUT/sub, sorted. */
  private static List<String> lines(String sub) throws IOException {
    List<String> all = new ArrayList<String>();
    File dir = new File(out, sub);
    File[] files = dir.listFiles();
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
    String rec = new String(Files.readAllBytes(new File(out, "clean/reconciliation.txt").toPath()), StandardCharsets.UTF_8);
    assertTrue(rec, rec.contains("input_lines(20) = blank(1) + header(1) + rejected(8) + filtered(2) + retained(8)\tPASS"));
    String agg = new String(Files.readAllBytes(new File(out, "aggregate/reconciliation.txt").toPath()), StandardCharsets.UTF_8);
    assertTrue(agg, agg.contains("zips_with_reports(3) + zero_rows(1) = reference_codes(4)\tPASS"));
  }

  @Test
  public void cleanedRecords() throws IOException {
    List<String> cleaned = lines("clean/cleaned");
    assertEquals(8, cleaned.size());
    assertTrue(cleaned.contains(
        "1013\t2025-06-15 15:45:00\t2025-06-15\tAnimal in a Park\tDog Off Leash\t07305\t07305\tMATCHED\tSTATEN ISLAND\t\\N\tSTATUS_MISSING"));
    // Duplicate key 1008: the earliest report is kept.
    assertTrue(cleaned.contains(
        "1008\t2025-05-01 12:00:00\t2025-05-01\tAnimal in a Park\tDog Off Leash\t11215\t11215\tMATCHED\tBROOKLYN\tClosed\t"));
    assertEquals(3, lines("clean/unmatched").size());
  }

  @Test
  public void rejectedAndFiltered() throws IOException {
    List<String> rejected = lines("clean/rejected");
    assertEquals(8, rejected.size());
    assertEquals(1, countReason(rejected, "DUPLICATE_ROW"));
    assertEquals(1, countReason(rejected, "DUPLICATE_KEY_CONFLICT"));
    assertEquals(1, countReason(rejected, "CREATED_DATE_OUTSIDE_2025"));
    assertEquals(1, countReason(rejected, "INVALID_CREATED_DATE"));
    assertEquals(1, countReason(rejected, "MISSING_UNIQUE_KEY"));
    assertEquals(1, countReason(rejected, "INVALID_UNIQUE_KEY"));
    assertEquals(1, countReason(rejected, "MALFORMED_CSV"));
    assertEquals(1, countReason(rejected, "FIELD_COUNT_MISMATCH"));
    List<String> filtered = lines("clean/filtered");
    assertEquals(1, countReason(filtered, "OTHER_DESCRIPTOR"));
    assertEquals(1, countReason(filtered, "OTHER_COMPLAINT_TYPE"));
  }

  @Test
  public void aggregateByZip() throws IOException {
    List<String> byZip = lines("aggregate/by_zip");
    assertEquals(4, byZip.size());
    assertTrue(byZip.contains("07305\t1\t0\t1\t2025-06-15\t2025-06-15\tSTATEN ISLAND"));
    assertTrue(byZip.contains("10001\t2\t1\t1\t2025-01-05\t2025-02-10\tMANHATTAN"));
    assertTrue(byZip.contains("10458\t0\t0\t0\t\\N\t\\N\t\\N"));
    assertTrue(byZip.contains("11215\t2\t2\t0\t2025-03-01\t2025-05-01\tBROOKLYN"));
  }

  @Test
  public void profiles() throws IOException {
    List<String> stats = lines("profile/raw/stats");
    assertTrue(stats.contains("_record\tINPUT_LINES\t20"));
    assertTrue(stats.contains("_record\tDATA_ROWS\t16"));
    assertTrue(stats.contains("unique_key\tMISSING\t1"));
    assertTrue(stats.contains("created_date\tMIN_TIMESTAMP\t2024-12-31 23:59:59"));
    assertTrue(stats.contains("incident_zip\tMISSING\t2"));
    List<String> freq = lines("profile/raw/freq");
    assertTrue(freq.contains("descriptor\tAnimal Abuse, Neglect\t1"));

    List<String> distinct = lines("profile/distinct/distinct");
    // unique_key: 13 distinct non-missing values; 1001 and 1008 repeat.
    assertTrue(distinct.toString(), distinct.contains("unique_key\t13\t1\t2\t2\t2"));
    assertTrue(distinct.toString(), distinct.contains("_row_md5\t15\t0\t1\t1\t2"));
    List<String> dups = lines("profile/distinct/duplicates");
    assertTrue(dups.contains("unique_key\t1001\t2"));
    assertTrue(dups.contains("unique_key\t1008\t2"));

    List<String> cleanedStats = lines("profile/cleaned/stats");
    assertTrue(cleanedStats.toString(), cleanedStats.contains("unique_key\tROWS\t8"));
    assertTrue(cleanedStats.toString(), cleanedStats.contains("incident_zip\tMISSING\t2"));
  }
}
