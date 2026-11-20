package edu.nyu.pawprint.d1;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * Job 3 (map-only): validate and standardize every license-period record and match the owner
 * ZIP to the D4 area reference. All extract years are kept; the ZIP-level baseline selects
 * one extract in {@link AggregateJob}.
 *
 * <p>Every input line ends up in exactly one place, and the driver checks this:
 * <pre>
 *   OUT/cleaned/part-*     retained license periods (Hive schema in hive/d1_dog_licensing_tables.hql)
 *   OUT/rejected/part-*    reasons  source  raw_line   (records with no usable license interval)
 *   OUT/unmatched/part-*   retained records whose ZIP is UNMATCHED / INVALID / MISSING (audit copy)
 *   OUT/reconciliation.txt input = blank + header + rejected + retained
 * </pre>
 *
 * <p>Hard rejections: malformed CSV, wrong field count, missing/invalid extract year, missing
 * or invalid issue date, issue date after the extract year, expiry before issue. ZIP, birth
 * year, sex, breed, name and long-duration problems do not reject a record; they are recorded
 * in {@code zip_status} and {@code quality_flags}, as in D5.
 *
 * <p>There is no published dog or license ID, so identical rows are not removed: a license
 * period is the unit of measurement (proposal section 5). {@link AggregateJob} reports counts
 * with and without collapsing identical rows.
 *
 * Usage: {@code hadoop jar d1-dog-licensing.jar edu.nyu.pawprint.d1.CleanJob <input> <zip-ref> <out>}
 */
public class CleanJob extends D1Tool {

  static final String G_INPUT = "D1_INPUT";
  static final String G_REJECTED = "D1_REJECTED";
  static final String G_RETAINED = "D1_RETAINED";
  static final String G_YEAR = "D1_RETAINED_BY_EXTRACT_YEAR";
  static final String G_ZIP = "D1_ZIP_STATUS";
  static final String G_FLAGS = "D1_QUALITY_FLAGS";

  /** Cleaned output columns, in order. */
  static final String[] CLEANED_COLUMNS = {
      "extract_year", "license_issued_date", "license_expired_date", "license_days",
      "owner_zip", "owner_zip_raw", "zip_status", "animal_birth_year", "animal_gender",
      "breed_name", "animal_name", "quality_flags"};
  static final int C_YEAR = 0, C_ZIP = 4, C_ZIP_STATUS = 6, C_FLAGS = 11;

  /** Result of evaluating one parsed record. */
  static final class Evaluation {
    final List<String> rejectReasons = new ArrayList<String>();
    String cleanedTsv;
    String extractYear;
    String zipStatus;
    final List<String> flags = new ArrayList<String>();

    boolean rejected() {
      return !rejectReasons.isEmpty();
    }
  }

  /** Applies all field rules to one record. Package-private for unit tests. */
  static Evaluation evaluate(List<String> fields, D1Schema.ColumnIndex cols, Set<String> zipRef) {
    Evaluation e = new Evaluation();

    String rawYear = cols.get(fields, D1Schema.EXTRACT_YEAR);
    Integer year = null;
    if (D1Rules.isMissing(rawYear)) {
      e.rejectReasons.add("MISSING_EXTRACT_YEAR");
    } else {
      year = D1Rules.parseExtractYear(rawYear);
      if (year == null) {
        e.rejectReasons.add("INVALID_EXTRACT_YEAR");
      }
    }

    String rawIssued = cols.get(fields, D1Schema.LICENSE_ISSUED);
    LocalDate issued = null;
    if (D1Rules.isMissing(rawIssued)) {
      e.rejectReasons.add("MISSING_ISSUE_DATE");
    } else {
      issued = D1Rules.parseDate(rawIssued);
      if (issued == null) {
        e.rejectReasons.add("INVALID_ISSUE_DATE");
      } else if (year != null && issued.getYear() > year) {
        e.rejectReasons.add("ISSUE_DATE_AFTER_EXTRACT_YEAR");
      }
    }

    String rawExpired = cols.get(fields, D1Schema.LICENSE_EXPIRED);
    LocalDate expired = null;
    if (D1Rules.isMissing(rawExpired)) {
      e.flags.add("EXPIRY_MISSING");
    } else {
      expired = D1Rules.parseDate(rawExpired);
      if (expired == null) {
        e.flags.add("EXPIRY_INVALID");
      } else if (issued != null && expired.isBefore(issued)) {
        e.rejectReasons.add("EXPIRY_BEFORE_ISSUE");
      }
    }
    if (e.rejected()) {
      return e;
    }

    String days = D1Rules.HIVE_NULL;
    if (expired != null) {
      days = Long.toString(expired.toEpochDay() - issued.toEpochDay());
      if (D1Rules.isOverFiveYears(issued, expired)) {
        e.flags.add("LICENSE_OVER_5_YEARS");
      }
    }

    // ZIP: standardize and match to the D4 spine.
    String rawZip = D1Rules.squash(cols.get(fields, D1Schema.ZIP_CODE));
    D1Rules.ZipStatus zs = D1Rules.zipStatus(rawZip, zipRef);
    String zip = D1Rules.normalizeZip(rawZip);
    if (zs == D1Rules.ZipStatus.MISSING || zs == D1Rules.ZipStatus.INVALID) {
      zip = null;
      e.flags.add("ZIP_" + zs.name());
    } else if (zs == D1Rules.ZipStatus.UNMATCHED) {
      e.flags.add("ZIP_NOT_IN_REFERENCE");
    }
    if (zip != null && D1Rules.isZipPlus4(rawZip)) {
      e.flags.add("ZIP_PLUS4_TRUNCATED");
    }
    if (zip != null && D1Rules.isLeadingZeroRestored(rawZip)) {
      e.flags.add("ZIP_LEADING_ZERO_RESTORED");
    }

    String rawBirth = cols.get(fields, D1Schema.ANIMAL_BIRTH_YEAR);
    Integer birth = D1Rules.parseBirthYear(rawBirth, issued.getYear());
    if (birth == null) {
      e.flags.add(D1Rules.isMissing(rawBirth) ? "BIRTH_YEAR_MISSING" : "BIRTH_YEAR_INVALID");
    }

    String rawGender = cols.get(fields, D1Schema.ANIMAL_GENDER);
    String gender = D1Rules.normalizeGender(rawGender);
    if (gender == null) {
      e.flags.add(D1Rules.isMissing(rawGender) ? "GENDER_MISSING" : "GENDER_UNRECOGNIZED");
    }

    String rawBreed = cols.get(fields, D1Schema.BREED_NAME);
    String breed = D1Rules.squash(rawBreed);
    if (D1Rules.isUnknownBreed(rawBreed)) {
      e.flags.add("BREED_UNKNOWN");
      breed = D1Rules.HIVE_NULL;
    }

    String rawName = cols.get(fields, D1Schema.ANIMAL_NAME);
    String name = D1Rules.squash(rawName).toUpperCase(Locale.ROOT);
    if (D1Rules.isNamePlaceholder(rawName)) {
      e.flags.add("NAME_MISSING");
      name = D1Rules.HIVE_NULL;
    }

    e.extractYear = Integer.toString(year);
    e.zipStatus = zs.name();
    String[] out = {
        e.extractYear,
        issued.format(D1Rules.OUT_DATE),
        expired == null ? D1Rules.HIVE_NULL : expired.format(D1Rules.OUT_DATE),
        days,
        zip == null ? D1Rules.HIVE_NULL : zip,
        rawZip.isEmpty() ? D1Rules.HIVE_NULL : rawZip,
        zs.name(),
        birth == null ? D1Rules.HIVE_NULL : birth.toString(),
        gender == null ? D1Rules.HIVE_NULL : gender,
        breed,
        name,
        join(e.flags, ";")};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < out.length; i++) {
      if (i > 0) {
        sb.append('\t');
      }
      sb.append(D1Schema.tsvSafe(out[i]));
    }
    e.cleanedTsv = sb.toString();
    return e;
  }

  static String join(List<String> parts, String sep) {
    StringBuilder sb = new StringBuilder();
    for (String p : parts) {
      if (sb.length() > 0) {
        sb.append(sep);
      }
      sb.append(p);
    }
    return sb.toString();
  }

  public static class CleanMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos;
    private D1Schema.ColumnIndex columns;
    private Set<String> zipRef;
    private String source;
    private final Text out = new Text();

    @Override
    protected void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      mos = new MultipleOutputs<NullWritable, Text>(context);
      columns = D1Schema.columns(conf);
      zipRef = ZipReference.load(conf);
      source = context.getInputSplit() instanceof FileSplit
          ? ((FileSplit) context.getInputSplit()).getPath().getName() : "input";
    }

    private void reject(String reasons, long offset, String line) throws IOException, InterruptedException {
      out.set(reasons + "\t" + source + ":" + offset + "\t" + D1Schema.tsvSafe(line));
      mos.write("rejected", NullWritable.get(), out, "rejected/part");
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if (offset.get() == 0) {
        line = CsvParser.stripBom(line);
      }
      context.getCounter(G_INPUT, "LINES").increment(1);
      if (line.trim().isEmpty()) {
        context.getCounter(G_INPUT, "BLANK_LINES").increment(1);
        return;
      }
      List<String> fields;
      try {
        fields = CsvParser.parseLine(line);
      } catch (CsvParser.MalformedCsvException ex) {
        context.getCounter(G_REJECTED, "MALFORMED_CSV").increment(1);
        reject("MALFORMED_CSV", offset.get(), line);
        return;
      }
      if (columns.isHeader(fields)) {
        context.getCounter(G_INPUT, "HEADER_LINES").increment(1);
        return;
      }
      if (fields.size() != columns.width()) {
        context.getCounter(G_REJECTED, "FIELD_COUNT_MISMATCH").increment(1);
        reject("FIELD_COUNT_MISMATCH", offset.get(), line);
        return;
      }

      Evaluation e = evaluate(fields, columns, zipRef);
      if (e.rejected()) {
        // Counted once under the first reason; all reasons are kept in the output.
        context.getCounter(G_REJECTED, e.rejectReasons.get(0)).increment(1);
        reject(join(e.rejectReasons, ";"), offset.get(), line);
        return;
      }
      context.getCounter(G_RETAINED, "RECORDS").increment(1);
      context.getCounter(G_YEAR, e.extractYear).increment(1);
      context.getCounter(G_ZIP, e.zipStatus).increment(1);
      for (String flag : e.flags) {
        context.getCounter(G_FLAGS, flag).increment(1);
      }
      if (e.flags.isEmpty()) {
        context.getCounter(G_FLAGS, "NONE").increment(1);
      }
      out.set(e.cleanedTsv);
      mos.write("cleaned", NullWritable.get(), out, "cleaned/part");
      if (!e.zipStatus.equals(D1Rules.ZipStatus.MATCHED.name())) {
        mos.write("unmatched", NullWritable.get(), out, "unmatched/part");
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: CleanJob <input> <zip-reference> <output>");
      return 2;
    }
    return runJob(getConf(), new Path(args[0]), new Path(args[1]), new Path(args[2])) ? 0 : 1;
  }

  static boolean runJob(Configuration base, Path input, Path zipRef, Path output) throws Exception {
    Configuration conf = new Configuration(base);
    D1Schema.resolveColumns(conf, input);
    conf.set(D1Schema.CONF_ZIP_REF_PATH, zipRef.toString());
    int refSize = ZipReference.load(conf).size(); // fail fast in the driver
    prepareOutput(conf, output);

    Job job = Job.getInstance(conf, "D1 dog licensing clean");
    job.setJarByClass(CleanJob.class);
    job.setMapperClass(CleanMapper.class);
    job.setNumReduceTasks(0); // map-only: no key to deduplicate on
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "cleaned", TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "unmatched", TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "rejected", TextOutputFormat.class, NullWritable.class, Text.class);
    if (!job.waitForCompletion(true)) {
      return false;
    }
    return reconcile(conf, job.getCounters(), new Path(output, "reconciliation.txt"), refSize);
  }

  /** Checks that every input line is accounted for and writes the report. */
  static boolean reconcile(Configuration conf, Counters c, Path report, int refSize) throws IOException {
    long lines = value(c, G_INPUT, "LINES");
    long blank = value(c, G_INPUT, "BLANK_LINES");
    long header = value(c, G_INPUT, "HEADER_LINES");
    long rejected = sum(c, G_REJECTED);
    long retained = value(c, G_RETAINED, "RECORDS");
    long zipTotal = sum(c, G_ZIP);
    long yearTotal = sum(c, G_YEAR);
    boolean linesOk = lines == blank + header + rejected + retained;
    boolean zipOk = zipTotal == retained && yearTotal == retained;

    StringBuilder sb = new StringBuilder();
    sb.append("# D1 dog licensing cleaning reconciliation\n");
    sb.append("zip_reference_codes\t").append(refSize).append('\n');
    appendGroup(sb, c, G_INPUT);
    appendGroup(sb, c, G_REJECTED);
    appendGroup(sb, c, G_RETAINED);
    appendGroup(sb, c, G_YEAR);
    appendGroup(sb, c, G_ZIP);
    appendGroup(sb, c, G_FLAGS);
    sb.append(String.format("CHECK\tinput_lines(%d) = blank(%d) + header(%d) + rejected(%d) + retained(%d)\t%s%n",
        lines, blank, header, rejected, retained, linesOk ? "PASS" : "FAIL"));
    sb.append(String.format("CHECK\tretained(%d) = sum(zip_status)(%d) = sum(extract_year)(%d)\t%s%n",
        retained, zipTotal, yearTotal, zipOk ? "PASS" : "FAIL"));
    writeReport(conf, report, sb.toString());
    System.out.print(sb);
    return linesOk && zipOk;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new CleanJob(), args));
  }
}
