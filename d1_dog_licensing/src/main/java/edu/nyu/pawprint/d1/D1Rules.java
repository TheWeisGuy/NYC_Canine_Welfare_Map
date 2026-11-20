package edu.nyu.pawprint.d1;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validation and standardization rules for D1 fields. Pure functions, unit tested.
 * Shared conventions follow {@code edu.nyu.pawprint.d5.D5Rules}: missing tokens, the Hive
 * null marker, ZIP normalization (five-character string, ZIP+4 truncated, 00000 invalid) and
 * the MATCHED / UNMATCHED / INVALID / MISSING ZIP status against the D4 reference.
 */
public final class D1Rules {

  private D1Rules() {}

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

  // ------------------------------------------------------------------- dates

  private static final List<DateTimeFormatter> DATE_FORMATS = Arrays.asList(
      // Portal CSV export: 09/12/2014
      DateTimeFormatter.ofPattern("MM/dd/uuuu", Locale.US).withResolverStyle(ResolverStyle.STRICT),
      DateTimeFormatter.ofPattern("M/d/uuuu", Locale.US).withResolverStyle(ResolverStyle.STRICT),
      // ISO date: 2014-09-12
      DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.US).withResolverStyle(ResolverStyle.STRICT));

  private static final List<DateTimeFormatter> DATE_TIME_FORMATS = Arrays.asList(
      // Socrata API: 2014-09-12T00:00:00.000
      DateTimeFormatter.ISO_LOCAL_DATE_TIME,
      DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.US).withResolverStyle(ResolverStyle.STRICT),
      DateTimeFormatter.ofPattern("MM/dd/uuuu hh:mm:ss a", Locale.US).withResolverStyle(ResolverStyle.STRICT));

  public static final DateTimeFormatter OUT_DATE = DateTimeFormatter.ofPattern("uuuu-MM-dd", Locale.US);

  /** Parses the supported date layouts; null if missing, unrecognized or impossible (Feb 30). */
  public static LocalDate parseDate(String raw) {
    String s = squash(raw);
    if (isMissing(s)) {
      return null;
    }
    if (s.endsWith("Z")) {
      s = s.substring(0, s.length() - 1);
    }
    s = s.toUpperCase(Locale.US);
    for (DateTimeFormatter f : DATE_FORMATS) {
      try {
        return LocalDate.parse(s, f);
      } catch (DateTimeParseException ignored) {
        // try next layout
      }
    }
    for (DateTimeFormatter f : DATE_TIME_FORMATS) {
      try {
        return LocalDateTime.parse(s, f).toLocalDate();
      } catch (DateTimeParseException ignored) {
        // try next layout
      }
    }
    return null;
  }

  /** DOHMH licenses run one to five years; anything past five years plus a month is flagged. */
  public static boolean isOverFiveYears(LocalDate issued, LocalDate expired) {
    return expired.isAfter(issued.plusYears(5).plusDays(31));
  }

  // ------------------------------------------------------------- extract year

  private static final Pattern YEAR = Pattern.compile("\\d{4}");

  /** Four-digit extract year in 2000..2100, or null. */
  public static Integer parseExtractYear(String raw) {
    String s = squash(raw);
    if (s.endsWith(".0")) {
      s = s.substring(0, s.length() - 2);
    }
    if (!YEAR.matcher(s).matches()) {
      return null;
    }
    int y = Integer.parseInt(s);
    return y >= 2000 && y <= 2100 ? y : null;
  }

  /** Birth year if it is a four-digit year no later than issue and at most 30 years before. */
  public static Integer parseBirthYear(String raw, int issueYear) {
    String s = squash(raw);
    if (!YEAR.matcher(s).matches()) {
      return null;
    }
    int y = Integer.parseInt(s);
    return y <= issueYear && y >= issueYear - 30 ? y : null;
  }

  // --------------------------------------------------------------------- ZIP

  /** Outcome of matching an owner ZIP against the D4 area reference (same values as D5). */
  public enum ZipStatus { MATCHED, UNMATCHED, INVALID, MISSING }

  private static final Pattern ZIP5 = Pattern.compile("\\d{5}");
  private static final Pattern ZIP4 = Pattern.compile("\\d{4}");
  private static final Pattern ZIP_PLUS4 = Pattern.compile("(\\d{5})-?\\d{4}");

  private static String zipDigits(String raw) {
    String s = squash(raw);
    if (s.endsWith(".0")) {
      s = s.substring(0, s.length() - 2);
    }
    return s;
  }

  /**
   * Five-character ZIP string, or null if unusable. ZIP+4 is cut to five digits. A four-digit
   * value gets its leading zero back (the export stores some NJ/CT/MA ZIPs as numbers,
   * e.g. 7302 for 07302). 00000 is invalid.
   */
  public static String normalizeZip(String raw) {
    String s = zipDigits(raw);
    String zip = null;
    if (ZIP5.matcher(s).matches()) {
      zip = s;
    } else if (ZIP4.matcher(s).matches()) {
      zip = "0" + s;
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

  public static boolean isZipPlus4(String raw) {
    return ZIP_PLUS4.matcher(zipDigits(raw)).matches();
  }

  public static boolean isLeadingZeroRestored(String raw) {
    return ZIP4.matcher(zipDigits(raw)).matches() && !zipDigits(raw).equals("0000");
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

  // ---------------------------------------------------- gender, breed, name

  /** "M" or "F" (also accepts MALE/FEMALE), or null if missing or unrecognized. */
  public static String normalizeGender(String raw) {
    String s = squash(raw).toUpperCase(Locale.ROOT);
    if (s.equals("M") || s.equals("MALE")) {
      return "M";
    }
    if (s.equals("F") || s.equals("FEMALE")) {
      return "F";
    }
    return null;
  }

  /** Blank, "Unknown" and "Not Provided" are breed placeholders, not breeds. */
  public static boolean isUnknownBreed(String raw) {
    String s = squash(raw).toUpperCase(Locale.ROOT);
    return isMissing(s) || s.equals("UNKNOWN") || s.equals("NOT PROVIDED");
  }

  /** Blank, "UNKNOWN" and "NAME NOT PROVIDED" are name placeholders. */
  public static boolean isNamePlaceholder(String raw) {
    String s = squash(raw).toUpperCase(Locale.ROOT);
    return isMissing(s) || s.equals("UNKNOWN") || s.equals("NAME NOT PROVIDED");
  }

  // ------------------------------------------------------------ type inference

  private static final Pattern INTEGER = Pattern.compile("[-+]?\\d+");
  private static final Pattern DECIMAL = Pattern.compile("[-+]?(\\d+\\.\\d*|\\.\\d+)([eE][-+]?\\d+)?");

  /** Profiling type of a single non-missing value: INTEGER, DECIMAL, DATE or STRING. */
  public static String inferType(String raw) {
    String s = squash(raw);
    if (INTEGER.matcher(s).matches()) {
      return "INTEGER";
    }
    if (DECIMAL.matcher(s).matches()) {
      return "DECIMAL";
    }
    if (parseDate(s) != null) {
      return "DATE";
    }
    return "STRING";
  }

  /** Shape of a value: digits become 9, letters A (e.g. 10025 -> 99999). */
  public static String shape(String raw) {
    String s = squash(raw);
    if (isMissing(s)) {
      return "<MISSING>";
    }
    return s.replaceAll("[0-9]", "9").replaceAll("[A-Za-z]", "A");
  }

  /** License duration bucket used in the profile. */
  public static String durationBucket(long days) {
    if (days <= 0) return "0_or_less";
    if (days <= 366) return "1_up_to_1y";
    if (days <= 731) return "2_1y_to_2y";
    if (days <= 1096) return "3_2y_to_3y";
    if (days <= 1461) return "4_3y_to_4y";
    if (days <= 1827) return "5_4y_to_5y";
    if (days <= 1858) return "6_5y_plus_31d_grace";
    return "7_over_5y_plus_31d";
  }
}
