package edu.nyu.pawprint.d5;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Column layout of the D5 311 extract plus the configuration keys shared by all jobs.
 *
 * <p>The column order is read from the header of the input file by the driver and passed to
 * tasks through the job configuration, so it works whether the extract came from the Socrata
 * API (unique_key, created_date, ...) or the portal export (Unique Key, Created Date, ...).
 */
public final class D5Schema {

  private D5Schema() {}

  // Configuration keys.
  public static final String CONF_COLUMNS = "d5.columns";
  public static final String CONF_PROFILE_COLUMNS = "d5.profile.columns";
  public static final String CONF_ZIP_REF_PATH = "d5.zip.ref.path";
  public static final String CONF_ZIP_REF_COLUMN = "d5.zip.ref.column";
  public static final String CONF_YEAR = "d5.year";
  public static final String CONF_OVERWRITE = "d5.overwrite";
  /** "csv" (default, the raw extract) or "tsv" (this project's cleaned outputs). */
  public static final String CONF_INPUT_FORMAT = "d5.input.format";
  public static final int DEFAULT_YEAR = 2025;

  // Canonical column names used in analysis.
  public static final String UNIQUE_KEY = "unique_key";
  public static final String CREATED_DATE = "created_date";
  public static final String COMPLAINT_TYPE = "complaint_type";
  public static final String DESCRIPTOR = "descriptor";
  public static final String INCIDENT_ZIP = "incident_zip";
  public static final String BOROUGH = "borough";
  public static final String STATUS = "status";

  public static final List<String> REQUIRED_COLUMNS = Collections.unmodifiableList(Arrays.asList(
      UNIQUE_KEY, CREATED_DATE, COMPLAINT_TYPE, DESCRIPTOR, INCIDENT_ZIP, BOROUGH, STATUS));

  /** Pseudo-column profiled as an MD5 of the whole line, for full-row duplicate detection. */
  public static final String FULL_ROW_HASH = "_row_md5";

  /** "Unique Key" / "unique_key" / "﻿Unique Key" all become "unique_key". */
  public static String normalizeName(String name) {
    String s = CsvParser.stripBom(name == null ? "" : name).trim().toLowerCase();
    s = s.replaceAll("[^a-z0-9]+", "_");
    return s.replaceAll("^_+|_+$", "");
  }

  /** Maps normalized column names to positions in a parsed record. */
  public static final class ColumnIndex {
    private final List<String> names;
    private final Map<String, Integer> positions = new HashMap<String, Integer>();

    public ColumnIndex(List<String> rawNames) {
      List<String> normalized = new ArrayList<String>();
      for (String raw : rawNames) {
        normalized.add(normalizeName(raw));
      }
      this.names = Collections.unmodifiableList(normalized);
      for (int i = 0; i < names.size(); i++) {
        if (!positions.containsKey(names.get(i))) {
          positions.put(names.get(i), i);
        }
      }
    }

    public int width() {
      return names.size();
    }

    public List<String> names() {
      return names;
    }

    public boolean has(String column) {
      return positions.containsKey(column);
    }

    /** Raw value of a column, or "" if the column is absent. */
    public String get(List<String> fields, String column) {
      Integer p = positions.get(column);
      return p == null || p >= fields.size() ? "" : fields.get(p);
    }

    /** True if the parsed record is (another copy of) the header line. */
    public boolean isHeader(List<String> fields) {
      if (fields.size() != names.size()) {
        return false;
      }
      for (int i = 0; i < fields.size(); i++) {
        if (!normalizeName(fields.get(i)).equals(names.get(i))) {
          return false;
        }
      }
      return true;
    }

    /** Throws if any required analysis column is missing from the header. */
    public void requireAll(List<String> required) throws IOException {
      List<String> missing = new ArrayList<String>();
      for (String c : required) {
        if (!has(c)) {
          missing.add(c);
        }
      }
      if (!missing.isEmpty()) {
        throw new IOException("D5 input is missing required columns " + missing + "; header columns are " + names);
      }
    }
  }

  /** Column index for task-side code (driver must have called {@link #resolveColumns}). */
  public static ColumnIndex columns(Configuration conf) throws IOException {
    String[] cols = conf.getStrings(CONF_COLUMNS);
    if (cols == null || cols.length == 0) {
      throw new IOException(CONF_COLUMNS + " is not set");
    }
    return new ColumnIndex(Arrays.asList(cols));
  }

  /**
   * Driver-side: if {@code d5.columns} is not set explicitly, reads the header line of the
   * first data file under {@code input} and stores the normalized column names in the conf.
   */
  public static ColumnIndex resolveColumns(Configuration conf, Path input) throws IOException {
    if (conf.getStrings(CONF_COLUMNS) == null) {
      FileSystem fs = input.getFileSystem(conf);
      Path first = firstDataFile(fs, input);
      if (first == null) {
        throw new IOException("No input files found under " + input);
      }
      try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(first), StandardCharsets.UTF_8))) {
        String header = r.readLine();
        if (header == null) {
          throw new IOException("Empty input file " + first);
        }
        List<String> fields;
        try {
          fields = CsvParser.parseLine(CsvParser.stripBom(header));
        } catch (CsvParser.MalformedCsvException e) {
          throw new IOException("Cannot parse header of " + first + ": " + e.getMessage(), e);
        }
        List<String> normalized = new ArrayList<String>();
        for (String f : fields) {
          normalized.add(normalizeName(f));
        }
        conf.setStrings(CONF_COLUMNS, normalized.toArray(new String[0]));
      }
    }
    ColumnIndex index = columns(conf);
    index.requireAll(REQUIRED_COLUMNS);
    return index;
  }

  private static Path firstDataFile(FileSystem fs, Path p) throws IOException {
    FileStatus st = fs.getFileStatus(p);
    if (st.isFile()) {
      return p;
    }
    FileStatus[] children = fs.listStatus(p);
    Arrays.sort(children);
    for (FileStatus c : children) {
      String name = c.getPath().getName();
      if (c.isFile() && !name.startsWith("_") && !name.startsWith(".")) {
        return c.getPath();
      }
    }
    return null;
  }

  /** Splits one input line according to {@code d5.input.format}. */
  public static List<String> split(Configuration conf, String line) throws CsvParser.MalformedCsvException {
    if ("tsv".equalsIgnoreCase(conf.get(CONF_INPUT_FORMAT, "csv"))) {
      return Arrays.asList(line.split("\t", -1));
    }
    return CsvParser.parseLine(line);
  }

  /** Makes a value safe for tab-separated output (no tabs or newlines). */
  public static String tsvSafe(String s) {
    return s == null ? "" : s.replaceAll("[\\t\\r\\n]+", " ");
  }
}
