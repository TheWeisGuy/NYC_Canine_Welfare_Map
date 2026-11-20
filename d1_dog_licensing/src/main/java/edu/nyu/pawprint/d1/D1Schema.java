package edu.nyu.pawprint.d1;

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
 * Column layout of the D1 NYC Dog Licensing Dataset (NYC Open Data nu7n-tubp) plus the
 * configuration keys shared by all D1 jobs. Mirrors {@code edu.nyu.pawprint.d5.D5Schema}.
 *
 * <p>The column order is read from the input header by the driver, so the portal export
 * ("AnimalName", ..., "Extract Year") and the Socrata API names ("animalname", ...,
 * "animalbirth", "extract_year") both work.
 */
public final class D1Schema {

  private D1Schema() {}

  // Configuration keys.
  public static final String CONF_COLUMNS = "d1.columns";
  public static final String CONF_PROFILE_COLUMNS = "d1.profile.columns";
  public static final String CONF_ZIP_REF_PATH = "d1.zip.ref.path";
  public static final String CONF_ZIP_REF_COLUMN = "d1.zip.ref.column";
  public static final String CONF_EXTRACT_YEAR = "d1.extract.year";
  public static final String CONF_OVERWRITE = "d1.overwrite";
  /** "csv" (default, the raw export) or "tsv" (this module's cleaned output). */
  public static final String CONF_INPUT_FORMAT = "d1.input.format";
  /** Extract used for the ZIP-level baseline (proposal section 5). */
  public static final int DEFAULT_EXTRACT_YEAR = 2026;

  // Canonical (normalized) raw column names.
  public static final String ANIMAL_NAME = "animalname";
  public static final String ANIMAL_GENDER = "animalgender";
  public static final String ANIMAL_BIRTH_YEAR = "animalbirthyear";
  public static final String BREED_NAME = "breedname";
  public static final String ZIP_CODE = "zipcode";
  public static final String LICENSE_ISSUED = "licenseissueddate";
  public static final String LICENSE_EXPIRED = "licenseexpireddate";
  public static final String EXTRACT_YEAR = "extract_year";

  public static final List<String> REQUIRED_COLUMNS = Collections.unmodifiableList(Arrays.asList(
      ANIMAL_NAME, ANIMAL_GENDER, ANIMAL_BIRTH_YEAR, BREED_NAME, ZIP_CODE,
      LICENSE_ISSUED, LICENSE_EXPIRED, EXTRACT_YEAR));

  /** Pseudo-column: extract year + MD5 of the whole line, for identical-row detection. */
  public static final String FULL_ROW_HASH = "_row_md5";
  /** Pseudo-column: extract|name|sex|birth year|breed|ZIP, a candidate "same dog" key. */
  public static final String CANDIDATE_DOG_KEY = "_candidate_dog_key";

  /** Other spellings of the same column (Socrata API field names, DOHMH user guide). */
  private static final Map<String, String> ALIASES = new HashMap<String, String>();

  static {
    ALIASES.put("animalbirth", ANIMAL_BIRTH_YEAR);
    ALIASES.put("animalyearofbirth", ANIMAL_BIRTH_YEAR);
    ALIASES.put("animal_birth_year", ANIMAL_BIRTH_YEAR);
    ALIASES.put("extractyear", EXTRACT_YEAR);
    ALIASES.put("zip_code", ZIP_CODE);
  }

  /** "Extract Year" / "extract_year" / "﻿AnimalName" -> "extract_year" / "animalname". */
  public static String normalizeName(String name) {
    String s = CsvParser.stripBom(name == null ? "" : name).trim().toLowerCase();
    s = s.replaceAll("[^a-z0-9]+", "_");
    s = s.replaceAll("^_+|_+$", "");
    String alias = ALIASES.get(s);
    return alias == null ? s : alias;
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
        throw new IOException("D1 input is missing required columns " + missing + "; header columns are " + names);
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
   * Driver-side: if {@code d1.columns} is not set explicitly, reads the header line of the
   * first data file under {@code input} and stores the normalized column names in the conf.
   * Raw CSV input must contain every required column; cleaned TSV input is not checked.
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
    if (!isTsv(conf)) {
      index.requireAll(REQUIRED_COLUMNS);
    }
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

  public static boolean isTsv(Configuration conf) {
    return "tsv".equalsIgnoreCase(conf.get(CONF_INPUT_FORMAT, "csv"));
  }

  /** Splits one input line according to {@code d1.input.format}. */
  public static List<String> split(Configuration conf, String line) throws CsvParser.MalformedCsvException {
    if (isTsv(conf)) {
      return Arrays.asList(line.split("\t", -1));
    }
    return CsvParser.parseLine(line);
  }

  /** Makes a value safe for tab-separated output (no tabs or newlines). */
  public static String tsvSafe(String s) {
    return s == null ? "" : s.replaceAll("[\\t\\r\\n]+", " ");
  }
}
