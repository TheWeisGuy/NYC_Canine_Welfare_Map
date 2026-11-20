package edu.nyu.pawprint.d1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.StringReader;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Test;

public class D1RulesTest {

  private static final Set<String> REF = new HashSet<String>(Arrays.asList("10001", "11215", "07305"));

  @Test
  public void dates() {
    assertEquals(LocalDate.of(2014, 9, 12), D1Rules.parseDate("09/12/2014"));
    assertEquals(LocalDate.of(2014, 9, 12), D1Rules.parseDate("9/12/2014"));
    assertEquals(LocalDate.of(2014, 9, 12), D1Rules.parseDate("2014-09-12"));
    assertEquals(LocalDate.of(2014, 9, 12), D1Rules.parseDate("2014-09-12T00:00:00.000"));
    assertNull(D1Rules.parseDate("02/30/2023"));
    assertNull(D1Rules.parseDate(""));
    assertNull(D1Rules.parseDate("soon"));
    assertTrue(D1Rules.isOverFiveYears(LocalDate.of(2018, 11, 23), LocalDate.of(2027, 6, 9)));
    assertFalse(D1Rules.isOverFiveYears(LocalDate.of(2020, 1, 1), LocalDate.of(2025, 1, 20)));
  }

  @Test
  public void years() {
    assertEquals(Integer.valueOf(2026), D1Rules.parseExtractYear(" 2026 "));
    assertEquals(Integer.valueOf(2026), D1Rules.parseExtractYear("2026.0"));
    assertNull(D1Rules.parseExtractYear("20X4"));
    assertNull(D1Rules.parseExtractYear("1999"));
    assertEquals(Integer.valueOf(2014), D1Rules.parseBirthYear("2014", 2014));
    assertNull(D1Rules.parseBirthYear("2015", 2014));          // born after issue
    assertNull(D1Rules.parseBirthYear("1983", 2014));          // older than 30 at issue
    assertNull(D1Rules.parseBirthYear("720", 2021));
    assertNull(D1Rules.parseBirthYear("ERROR: #VALUE!", 2021));
  }

  @Test
  public void zips() {
    assertEquals("10001", D1Rules.normalizeZip(" 10001 "));
    assertEquals("10001", D1Rules.normalizeZip("10001-1234"));
    assertEquals("07305", D1Rules.normalizeZip("7305"));
    assertEquals("10001", D1Rules.normalizeZip("10001.0"));
    assertNull(D1Rules.normalizeZip("923"));
    assertNull(D1Rules.normalizeZip("0"));
    assertNull(D1Rules.normalizeZip("00000"));
    assertTrue(D1Rules.isZipPlus4("10001-1234"));
    assertTrue(D1Rules.isLeadingZeroRestored("7305"));
    assertFalse(D1Rules.isLeadingZeroRestored("10001"));
    assertEquals(D1Rules.ZipStatus.MATCHED, D1Rules.zipStatus("7305", REF));
    assertEquals(D1Rules.ZipStatus.UNMATCHED, D1Rules.zipStatus("10002", REF));
    assertEquals(D1Rules.ZipStatus.INVALID, D1Rules.zipStatus("923", REF));
    assertEquals(D1Rules.ZipStatus.MISSING, D1Rules.zipStatus(" ", REF));
  }

  @Test
  public void placeholders() {
    assertEquals("M", D1Rules.normalizeGender(" m "));
    assertEquals("F", D1Rules.normalizeGender("Female"));
    assertNull(D1Rules.normalizeGender(""));
    assertTrue(D1Rules.isUnknownBreed("Unknown"));
    assertTrue(D1Rules.isUnknownBreed("Not Provided"));
    assertFalse(D1Rules.isUnknownBreed("Bull Dog, French"));
    assertTrue(D1Rules.isNamePlaceholder("NAME NOT PROVIDED"));
    assertTrue(D1Rules.isNamePlaceholder("unknown"));
    assertFalse(D1Rules.isNamePlaceholder("BELLA"));
  }

  @Test
  public void columnNames() {
    assertEquals("extract_year", D1Schema.normalizeName("Extract Year"));
    assertEquals("animalname", D1Schema.normalizeName("﻿AnimalName"));
    assertEquals("animalbirthyear", D1Schema.normalizeName("animalbirth"));      // Socrata API name
    assertEquals("animalbirthyear", D1Schema.normalizeName("AnimalYearOfBirth")); // user guide name
  }

  @Test
  public void evaluateKeepsQuotedCommaAndFlags() throws Exception {
    D1Schema.ColumnIndex cols = new D1Schema.ColumnIndex(Arrays.asList(
        "AnimalName", "AnimalGender", "AnimalBirthYear", "BreedName", "ZipCode",
        "LicenseIssuedDate", "LicenseExpiredDate", "Extract Year"));
    List<String> f = CsvParser.parseLine(
        "\"CREAM\",\"M\",\"720\",\"Jindo Dog, Korea\",\"11215\",\"02/19/2021\",\"02/19/2026\",\"2026\"");
    CleanJob.Evaluation e = CleanJob.evaluate(f, cols, REF);
    assertFalse(e.rejected());
    assertEquals("2026\t2021-02-19\t2026-02-19\t1826\t11215\t11215\tMATCHED\t\\N\tM\tJindo Dog, Korea\tCREAM\tBIRTH_YEAR_INVALID",
        e.cleanedTsv);

    List<String> inverted = CsvParser.parseLine(
        "\"ROCKY\",\"M\",\"2015\",\"Boxer\",\"11215\",\"05/01/2023\",\"04/01/2023\",\"2024\"");
    assertEquals(Arrays.asList("EXPIRY_BEFORE_ISSUE"), CleanJob.evaluate(inverted, cols, REF).rejectReasons);
  }

  @Test
  public void referenceWithMultiLineGeometry() throws Exception {
    String csv = "the_geom,Zip Code\n\"MULTIPOLYGON (((-73.9 40.7,\n -73.8 40.7)))\",10001\n\"POLYGON ((1 2, 3 4))\",07305\n";
    Set<String> codes = ZipReference.read(new BufferedReader(new StringReader(csv)), null);
    assertEquals(new HashSet<String>(Arrays.asList("10001", "07305")), codes);
  }
}
