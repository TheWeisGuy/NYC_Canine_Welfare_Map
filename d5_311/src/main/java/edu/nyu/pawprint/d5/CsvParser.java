package edu.nyu.pawprint.d5;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * RFC 4180 CSV parser. Quoted fields may contain commas, doubled quotes ("") and,
 * when read through {@link #readRecord(BufferedReader)}, embedded newlines (needed
 * for WKT geometry columns). Lines are never split naively on commas.
 */
public final class CsvParser {

  private CsvParser() {}

  /** Thrown when a line is not valid CSV. */
  public static final class MalformedCsvException extends Exception {
    private final boolean unterminatedQuote;

    MalformedCsvException(String message, boolean unterminatedQuote) {
      super(message);
      this.unterminatedQuote = unterminatedQuote;
    }

    /** True when the record may continue on the next physical line. */
    public boolean isUnterminatedQuote() {
      return unterminatedQuote;
    }
  }

  /** Parses one complete CSV record. */
  public static List<String> parseLine(String line) throws MalformedCsvException {
    List<String> fields = new ArrayList<String>();
    StringBuilder sb = new StringBuilder();
    boolean inQuotes = false;
    boolean afterClosingQuote = false;
    int n = line.length();
    int i = 0;
    while (i < n) {
      char c = line.charAt(i);
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < n && line.charAt(i + 1) == '"') {
            sb.append('"');
            i += 2;
            continue;
          }
          inQuotes = false;
          afterClosingQuote = true;
        } else {
          sb.append(c);
        }
        i++;
        continue;
      }
      if (c == ',') {
        fields.add(sb.toString());
        sb.setLength(0);
        afterClosingQuote = false;
      } else if (c == '\r' && i == n - 1) {
        // Trailing CR from CRLF files.
      } else if (afterClosingQuote) {
        if (c != ' ' && c != '\t') {
          throw new MalformedCsvException("character after closing quote at position " + i, false);
        }
      } else if (c == '"') {
        if (sb.toString().trim().isEmpty()) {
          sb.setLength(0); // tolerate whitespace before an opening quote
          inQuotes = true;
        } else {
          throw new MalformedCsvException("stray quote inside unquoted field at position " + i, false);
        }
      } else {
        sb.append(c);
      }
      i++;
    }
    if (inQuotes) {
      throw new MalformedCsvException("unterminated quoted field", true);
    }
    fields.add(sb.toString());
    return fields;
  }

  /**
   * Reads one logical record, joining physical lines while a quoted field is open.
   * Returns null at end of input.
   */
  public static List<String> readRecord(BufferedReader reader) throws IOException, MalformedCsvException {
    String line = reader.readLine();
    if (line == null) {
      return null;
    }
    StringBuilder record = new StringBuilder(line);
    while (true) {
      try {
        return parseLine(record.toString());
      } catch (MalformedCsvException e) {
        if (!e.isUnterminatedQuote()) {
          throw e;
        }
        String next = reader.readLine();
        if (next == null) {
          throw e;
        }
        record.append('\n').append(next);
      }
    }
  }

  /** Removes a UTF-8 byte order mark if present. */
  public static String stripBom(String s) {
    if (s != null && !s.isEmpty() && s.charAt(0) == '﻿') {
      return s.substring(1);
    }
    return s;
  }
}
