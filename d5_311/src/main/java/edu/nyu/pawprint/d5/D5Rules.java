package edu.nyu.pawprint.d5;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Validation and standardization rules for D5 fields. Pure functions, unit tested. */
public final class D5Rules {

  private D5Rules() {}

  public static final String TARGET_COMPLAINT_TYPE = "Animal in a Park";
  public static final String TARGET_DESCRIPTOR = "Dog Off Leash";

  /** Hive TEXTFILE null marker. */
  public static final String HIVE_NULL = "\\N";

  private static final Set<String> MISSING_TOKENS =
      new HashSet<String>(Arrays.asList("", "null", "none", "n/a", "na", "nan", "\\n"));

  /** Trim and collapse internal whitespace. */
  public static String squash(String s) {
    return s == null ? "" : s.trim().replaceAll("\\s+", " ");
  }

  public static boolean isMissing(String s) {
    return MISSING_TOKENS.contains(squash(s).toLowerCase(Locale.ROOT));
  }

  // ---------------------------------------------------------------- unique_key

  private static final Pattern DIGITS = Pattern.compile("\\d{1,18}");

  /** Positive integer request ID, or null if invalid. Accepts a trailing ".0" from spreadsheets. */
  public static Long parseUniqueKey(String raw) {
    String s = squash(raw);
    if (s.endsWith(".0")) {
      s = s.substring(0, s.length() - 2);
    }
    if (!DIGITS.matcher(s).matches()) {
      return null;
    }
    long v = Long.parseLong(s);
    return v > 0 ? v : null;
  }

  // -------------------------------------------------------------- created_date

  private static final List<DateTimeFormatter> TIMESTAMP_FORMATS = Arrays.asList(
      // Socrata API: 2025-01-01T13:45:00.000
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
      // 2025-01-01 13:45:00[.fraction]
      new DateTimeFormatterBuilder()
          .appendPattern("uuuu-MM-dd HH:mm:ss")
          .optionalStart().appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true).optionalEnd()
          .toFormatter(Locale.US).withResolverStyle(ResolverStyle.STRICT),
      // Portal CSV export: 01/01/2025 01:45:00 PM
      DateTimeFormatter.ofPattern("MM/dd/uuuu hh:mm:ss a", Locale.US).withResolverStyle(ResolverStyle.STRICT),
      DateTimeFormatter.ofPattern("MM/dd/uuuu HH:mm:ss", Locale.US).withResolverStyle(ResolverStyle.STRICT),
      DateTimeFormatter.ofPattern("MM/dd/uuuu HH:mm", Locale.US).withResolverStyle(ResolverStyle.STRICT));

  public static final DateTimeFormatter OUT_TIMESTAMP = DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.US);
  public static final DateTimeFormatter OUT_DATE = DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.US);

  /** Parses the supported 311 timestamp layouts; null if none match (or the date is impossible). */
  public static LocalDateTime parseTimestamp(String raw) {
    String s = squash(raw);
    if (s.endsWith("Z")) {
      s = s.substring(0, s.length() - 1);
    }
    s = s.toUpperCase(Locale.US); // "pm" -> "PM"
    for (DateTimeFormatter f : TIMESTAMP_FORMATS) {
      try {
        return LocalDateTime.parse(s, f);
      } catch (DateTimeParseException ignored) {
        // try next layout
      }
    }
    return null;
  }

  // -------------------------------------------------------------- incident_zip

  /** Outcome of matching a 311 ZIP against the D4 area reference. */
  public enum ZipStatus { MATCHED, UNMATCHED, INVALID, MISSING }

  private static final Pattern ZIP5 = Pattern.compile("\\d{5}");
  private static final Pattern ZIP_PLUS4 = Pattern.compile("(\\d{5})-?\\d{4}");

  /** Five-character ZIP string, or null if the value is not a usable five-digit ZIP. */
  public static String normalizeZip(String raw) {
    String s = squash(raw);
    if (s.endsWith(".0")) {
      s = s.substring(0, s.length() - 2);
    }
    String zip = null;
    if (ZIP5.matcher(s).matches()) {
      zip = s;
    } else {
      Matcher m = ZIP_PLUS4.matcher(s);
      if (m.matches()) {
        zip = m.group(1);
      }
    }
    if (zip == null || zip.equals("00000")) {
      return null;
    }
    return zip;
  }

  /** True when the raw value was ZIP+4 and was truncated to five digits. */
  public static boolean isZipPlus4(String raw) {
    return ZIP_PLUS4.matcher(squash(raw)).matches();
  }

  public static ZipStatus zipStatus(String raw, Set<String> reference) {
    if (isMissing(raw)) {
      return ZipStatus.MISSING;
    }
    String zip = normalizeZip(raw);
    if (zip == null) {
      return ZipStatus.INVALID;
    }
    return reference.contains(zip) ? ZipStatus.MATCHED : ZipStatus.UNMATCHED;
  }

  // ------------------------------------------------------------------- borough

  public static final String BOROUGH_UNSPECIFIED = "UNSPECIFIED";
  private static final Map<String, String> BOROUGHS = new HashMap<String, String>();

  static {
    for (String b : new String[] {"MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"}) {
      BOROUGHS.put(b, b);
    }
    BOROUGHS.put("NEW YORK", "MANHATTAN");
    BOROUGHS.put("THE BRONX", "BRONX");
    BOROUGHS.put("KINGS", "BROOKLYN");
    BOROUGHS.put("RICHMOND", "STATEN ISLAND");
    BOROUGHS.put("STATENISLAND", "STATEN ISLAND");
  }

  /** Canonical upper-case borough, {@link #BOROUGH_UNSPECIFIED}, or null if unrecognized. */
  public static String normalizeBorough(String raw) {
    if (isMissing(raw)) {
      return BOROUGH_UNSPECIFIED;
    }
    String s = squash(raw).toUpperCase(Locale.ROOT);
    if (s.equals("UNSPECIFIED")) {
      return BOROUGH_UNSPECIFIED;
    }
    return BOROUGHS.get(s);
  }

  // -------------------------------------------------------------------- status

  private static final Map<String, String> STATUSES = new HashMap<String, String>();

  static {
    for (String s : new String[] {"Open", "Closed", "Pending", "Assigned", "Started", "In Progress", "Unspecified"}) {
      STATUSES.put(s.toUpperCase(Locale.ROOT), s);
    }
  }

  /** Canonical status label, or null if unrecognized. */
  public static String normalizeStatus(String raw) {
    return STATUSES.get(squash(raw).toUpperCase(Locale.ROOT));
  }

  // ------------------------------------------------- complaint type / descriptor

  private static String matchKey(String s) {
    return squash(s).toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", " ").trim();
  }

  public static boolean isAnimalInPark(String complaintType) {
    return matchKey(complaintType).equals(matchKey(TARGET_COMPLAINT_TYPE));
  }

  /** Matches "Dog Off Leash", "dog off-leash", "Dog  Off Leash" etc. */
  public static boolean isDogOffLeash(String descriptor) {
    return matchKey(descriptor).equals(matchKey(TARGET_DESCRIPTOR));
  }

  // ------------------------------------------------------------ type inference

  private static final Pattern INTEGER = Pattern.compile("[-+]?\\d+");
  private static final Pattern DECIMAL = Pattern.compile("[-+]?(\\d+\\.\\d*|\\.\\d+)([eE][-+]?\\d+)?");

  /** Profiling type of a single non-missing value: INTEGER, DECIMAL, TIMESTAMP or STRING. */
  public static String inferType(String raw) {
    String s = squash(raw);
    if (INTEGER.matcher(s).matches()) {
      return "INTEGER";
    }
    if (DECIMAL.matcher(s).matches()) {
      return "DECIMAL";
    }
    if (parseTimestamp(s) != null) {
      return "TIMESTAMP";
    }
    return "STRING";
  }
}
