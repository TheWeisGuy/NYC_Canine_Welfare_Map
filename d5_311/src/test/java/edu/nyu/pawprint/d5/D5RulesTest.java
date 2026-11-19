package edu.nyu.pawprint.d5;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

public class D5RulesTest {

  @Test
  public void uniqueKey() {
    assertEquals(Long.valueOf(63813025L), D5Rules.parseUniqueKey(" 63813025 "));
    assertEquals(Long.valueOf(5L), D5Rules.parseUniqueKey("5.0"));
    assertNull(D5Rules.parseUniqueKey("ABC"));
    assertNull(D5Rules.parseUniqueKey("0"));
    assertNull(D5Rules.parseUniqueKey("-4"));
  }

  @Test
  public void timestamps() {
    assertEquals("2025-01-05 08:30:00",
        D5Rules.parseTimestamp("2025-01-05T08:30:00.000").format(D5Rules.OUT_TIMESTAMP));
    assertEquals("2025-06-15 15:45:00",
        D5Rules.parseTimestamp("06/15/2025 03:45:00 PM").format(D5Rules.OUT_TIMESTAMP));
    assertEquals("2025-06-15 15:45:00",
        D5Rules.parseTimestamp("2025-06-15 15:45:00").format(D5Rules.OUT_TIMESTAMP));
    assertNull(D5Rules.parseTimestamp("2025-02-30T10:00:00.000"));
    assertNull(D5Rules.parseTimestamp("yesterday"));
  }

  @Test
  public void zips() {
    Set<String> ref = new HashSet<String>(Arrays.asList("10001", "07305"));
    assertEquals("11215", D5Rules.normalizeZip("11215-1234"));
    assertEquals("11215", D5Rules.normalizeZip("112151234"));
    assertEquals("10001", D5Rules.normalizeZip("10001.0"));
    assertNull(D5Rules.normalizeZip("00000"));
    assertNull(D5Rules.normalizeZip("1000"));
    assertEquals(D5Rules.ZipStatus.MATCHED, D5Rules.zipStatus("07305", ref));
    assertEquals(D5Rules.ZipStatus.UNMATCHED, D5Rules.zipStatus("99999", ref));
    assertEquals(D5Rules.ZipStatus.INVALID, D5Rules.zipStatus("NYC", ref));
    assertEquals(D5Rules.ZipStatus.MISSING, D5Rules.zipStatus("", ref));
    assertEquals(D5Rules.ZipStatus.MISSING, D5Rules.zipStatus("N/A", ref));
  }

  @Test
  public void categoricals() {
    assertEquals("STATEN ISLAND", D5Rules.normalizeBorough("Staten  Island"));
    assertEquals(D5Rules.BOROUGH_UNSPECIFIED, D5Rules.normalizeBorough("Unspecified"));
    assertNull(D5Rules.normalizeBorough("Jersey"));
    assertEquals("In Progress", D5Rules.normalizeStatus("IN PROGRESS"));
    assertTrue(D5Rules.isDogOffLeash("dog off-leash"));
    assertFalse(D5Rules.isDogOffLeash("Dog Bite"));
    assertTrue(D5Rules.isAnimalInPark("ANIMAL IN A PARK"));
    assertTrue(D5Rules.isMissing("\\N"));
  }

  @Test
  public void typeInference() {
    assertEquals("INTEGER", D5Rules.inferType("10001"));
    assertEquals("DECIMAL", D5Rules.inferType("1.5"));
    assertEquals("TIMESTAMP", D5Rules.inferType("2025-01-05T08:30:00.000"));
    assertEquals("STRING", D5Rules.inferType("Dog Off Leash"));
  }

  @Test
  public void evaluateRetainsWithQualityFlags() {
    D5Schema.ColumnIndex cols = new D5Schema.ColumnIndex(D5Schema.REQUIRED_COLUMNS);
    Set<String> ref = Collections.singleton("11215");
    CleanJob.Evaluation e = CleanJob.evaluate(
        Arrays.asList("1003", "2025-03-01T09:15:00.000", "Animal in a Park", "dog off-leash", "11215-1234", "brooklyn", "closed"),
        cols, ref, 2025);
    assertFalse(e.rejected());
    assertNull(e.filterReason);
    assertEquals("1003\t2025-03-01 09:15:00\t2025-03-01\tAnimal in a Park\tDog Off Leash\t11215\t11215-1234\tMATCHED\tBROOKLYN\tClosed\tZIP_PLUS4_TRUNCATED",
        e.cleanedTsv);
  }

  @Test
  public void evaluateCollectsAllRejectReasons() {
    D5Schema.ColumnIndex cols = new D5Schema.ColumnIndex(D5Schema.REQUIRED_COLUMNS);
    CleanJob.Evaluation e = CleanJob.evaluate(
        Arrays.asList("x", "2024-01-01T00:00:00", "", "Dog Off Leash", "10001", "QUEENS", "Open"),
        cols, Collections.<String>emptySet(), 2025);
    assertEquals(Arrays.asList("INVALID_UNIQUE_KEY", "CREATED_DATE_OUTSIDE_2025", "MISSING_COMPLAINT_TYPE"), e.rejectReasons);
  }

  @Test
  public void zipReferenceFormats() throws Exception {
    assertEquals(new HashSet<String>(Arrays.asList("10001", "11215")),
        ZipReference.read(new BufferedReader(new StringReader("the_geom,ZCTA\n\"POLY (1 2,\n3 4)\",10001\n\"P\",11215\n")), null));
    assertEquals(new HashSet<String>(Arrays.asList("10001", "11215")),
        ZipReference.read(new BufferedReader(new StringReader("10001\n11215\n")), null));
    assertEquals(Collections.singleton("10001"), ZipReference.extract("860Z200US10001"));
  }
}
