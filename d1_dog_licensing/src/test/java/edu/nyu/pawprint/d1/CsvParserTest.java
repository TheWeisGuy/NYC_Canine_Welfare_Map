package edu.nyu.pawprint.d1;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class CsvParserTest {

  @Test
  public void keepsQuotedCommasAndEscapedQuotes() throws Exception {
    List<String> f = CsvParser.parseLine("1,\"Animal Abuse, Neglect\",\"say \"\"hi\"\"\",");
    assertEquals(Arrays.asList("1", "Animal Abuse, Neglect", "say \"hi\"", ""), f);
  }

  @Test
  public void keepsWktGeometry() throws Exception {
    List<String> f = CsvParser.parseLine("\"MULTIPOLYGON (((-73.9 40.7, -73.8 40.7)))\",10001");
    assertEquals(2, f.size());
    assertEquals("10001", f.get(1));
  }

  @Test
  public void handlesTrailingCarriageReturn() throws Exception {
    assertEquals(Arrays.asList("a", "b"), CsvParser.parseLine("a,b\r"));
  }

  @Test
  public void rejectsUnterminatedQuote() {
    try {
      CsvParser.parseLine("1,\"open,2");
      fail();
    } catch (CsvParser.MalformedCsvException e) {
      assertTrue(e.isUnterminatedQuote());
    }
  }

  @Test(expected = CsvParser.MalformedCsvException.class)
  public void rejectsGarbageAfterClosingQuote() throws Exception {
    CsvParser.parseLine("\"a\"b,c");
  }

  @Test
  public void readRecordJoinsMultiLineFields() throws Exception {
    BufferedReader r = new BufferedReader(new StringReader("\"POLY (1 2,\n 3 4)\",10001\nx,y\n"));
    assertEquals(Arrays.asList("POLY (1 2,\n 3 4)", "10001"), CsvParser.readRecord(r));
    assertEquals(Arrays.asList("x", "y"), CsvParser.readRecord(r));
    assertEquals(null, CsvParser.readRecord(r));
  }
}
