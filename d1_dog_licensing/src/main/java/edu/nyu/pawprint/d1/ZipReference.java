package edu.nyu.pawprint.d1;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Loads the set of five-digit area codes from the D4 ZCTA reference (the area spine).
 *
 * <p>Accepts either the raw NYC OTI CSV (with WKT geometry, possibly spanning lines) or
 * Member 3's cleaned reference. The code column is taken from {@code d1.zip.ref.column} if set,
 * otherwise auto-detected from common names; a headerless file uses the first column that
 * holds five-digit codes.
 */
public final class ZipReference {

  private ZipReference() {}

  static final List<String> CANDIDATE_COLUMNS = Arrays.asList(
      "zcta", "zcta5", "zcta5ce20", "zcta5ce10", "zcta20", "modzcta", "zipcode", "zip_code",
      "zip", "postalcode", "postal_code", "geoid20", "geoid", "geo_id");

  private static final Pattern ZIP5 = Pattern.compile("\\d{5}");
  private static final Pattern GEOID = Pattern.compile(".*US(\\d{5})");

  public static Set<String> load(Configuration conf) throws IOException {
    String p = conf.get(D1Schema.CONF_ZIP_REF_PATH);
    if (p == null) {
      throw new IOException(D1Schema.CONF_ZIP_REF_PATH + " is not set");
    }
    Path path = new Path(p);
    FileSystem fs = path.getFileSystem(conf);
    Set<String> codes = new TreeSet<String>();
    for (Path file : dataFiles(fs, path)) {
      try (BufferedReader r = new BufferedReader(new InputStreamReader(fs.open(file), StandardCharsets.UTF_8))) {
        codes.addAll(read(r, conf.get(D1Schema.CONF_ZIP_REF_COLUMN)));
      }
    }
    if (codes.isEmpty()) {
      throw new IOException("ZIP reference " + path + " contained no five-digit codes");
    }
    return codes;
  }

  /** Parses one reference file. Package-private for tests. */
  static Set<String> read(BufferedReader reader, String configuredColumn) throws IOException {
    Set<String> codes = new TreeSet<String>();
    List<String> first;
    try {
      first = CsvParser.readRecord(reader);
    } catch (CsvParser.MalformedCsvException e) {
      throw new IOException("Cannot parse ZIP reference header: " + e.getMessage(), e);
    }
    if (first == null) {
      return codes;
    }
    D1Schema.ColumnIndex header = new D1Schema.ColumnIndex(first);
    int col = -1;
    if (configuredColumn != null && !configuredColumn.trim().isEmpty()) {
      col = header.names().indexOf(D1Schema.normalizeName(configuredColumn));
      if (col < 0) {
        throw new IOException("ZIP reference column '" + configuredColumn + "' not in header " + header.names());
      }
    } else {
      for (String candidate : CANDIDATE_COLUMNS) {
        col = header.names().indexOf(candidate);
        if (col >= 0) {
          break;
        }
      }
    }
    if (col < 0) {
      // Headerless: use the first field holding codes and count this record as data.
      for (int i = 0; i < first.size() && col < 0; i++) {
        if (!extract(first.get(i)).isEmpty()) {
          col = i;
        }
      }
      if (col < 0) {
        throw new IOException("Cannot find a ZIP/ZCTA column in reference header " + header.names()
            + "; set -D " + D1Schema.CONF_ZIP_REF_COLUMN + "=<column>");
      }
      codes.addAll(extract(first.get(col)));
    }
    while (true) {
      List<String> rec;
      try {
        rec = CsvParser.readRecord(reader);
      } catch (CsvParser.MalformedCsvException e) {
        throw new IOException("Malformed ZIP reference record: " + e.getMessage(), e);
      }
      if (rec == null) {
        break;
      }
      if (col < rec.size()) {
        codes.addAll(extract(rec.get(col)));
      }
    }
    return codes;
  }

  /** "10001" -> {10001}; "860Z200US10001" -> {10001}; "10001, 10118" -> {10001, 10118}. */
  static Set<String> extract(String value) {
    Set<String> out = new TreeSet<String>();
    String s = value == null ? "" : value.trim();
    Matcher g = GEOID.matcher(s);
    if (ZIP5.matcher(s).matches()) {
      out.add(s);
    } else if (g.matches()) {
      out.add(g.group(1));
    } else if (s.length() < 200) { // never scan WKT geometry text
      for (String token : s.split("[,;\\s]+")) {
        if (ZIP5.matcher(token).matches()) {
          out.add(token);
        }
      }
    }
    return out;
  }

  private static List<Path> dataFiles(FileSystem fs, Path p) throws IOException {
    FileStatus st = fs.getFileStatus(p);
    java.util.ArrayList<Path> out = new java.util.ArrayList<Path>();
    if (st.isFile()) {
      out.add(p);
      return out;
    }
    for (FileStatus c : fs.listStatus(p)) {
      String name = c.getPath().getName();
      if (c.isFile() && !name.startsWith("_") && !name.startsWith(".")) {
        out.add(c.getPath());
      }
    }
    return out;
  }
}
