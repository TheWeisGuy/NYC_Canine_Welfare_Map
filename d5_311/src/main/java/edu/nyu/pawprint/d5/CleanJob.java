package edu.nyu.pawprint.d5;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * Job 3: validate, standardize and filter the 2025 Animal in a Park extract to Dog Off Leash
 * reports, match ZIPs to the D4 area reference and remove duplicate request IDs.
 *
 * <p>Every input line ends up in exactly one place, and the driver checks this:
 * <pre>
 *   OUT/cleaned/part-*     retained Dog Off Leash reports (Hive schema in hive/d5_311_tables.hql)
 *   OUT/rejected/part-*    reasons  source  raw_line   (invalid records and duplicate IDs)
 *   OUT/filtered/part-*    reason   source  raw_line   (valid but out of scope: not Dog Off Leash)
 *   OUT/unmatched/part-*   retained reports whose ZIP is missing, invalid or not in D4 (audit copy)
 *   OUT/reconciliation.txt input = blank + header + rejected + filtered + retained
 * </pre>
 *
 * <p>Hard rejections: malformed CSV, wrong field count, missing/invalid unique_key, missing,
 * unparseable or out-of-period created_date, missing complaint_type or descriptor, duplicate
 * unique_key. ZIP, borough and status problems do not reject a report; they are recorded in
 * {@code zip_status} and {@code quality_flags} so citywide counts stay complete.
 *
 * Usage: {@code hadoop jar d5-311.jar edu.nyu.pawprint.d5.CleanJob <input> <zip-ref> <out>}
 */
public class CleanJob extends D5Tool {

  static final String G_INPUT = "D5_INPUT";
  static final String G_REJECTED = "D5_REJECTED";
  static final String G_FILTERED = "D5_FILTERED";
  static final String G_RETAINED = "D5_RETAINED";
  static final String G_ZIP = "D5_ZIP_STATUS";
  static final String G_FLAGS = "D5_QUALITY_FLAGS";

  static final String SEP = "\u0001";

  /** Cleaned output columns, in order. */
  static final String[] CLEANED_COLUMNS = {
      "unique_key", "created_ts", "created_date", "complaint_type", "descriptor", "incident_zip",
      "incident_zip_raw", "zip_status", "borough", "status", "quality_flags"};

  /** Result of evaluating one parsed record. */
  static final class Evaluation {
    final List<String> rejectReasons = new ArrayList<String>();
    String filterReason;
    String uniqueKey;
    String cleanedTsv;
    String zipStatus;
    List<String> flags = new ArrayList<String>();

    boolean rejected() {
      return !rejectReasons.isEmpty();
    }
  }

  /** Applies all field rules to one record. Package-private for unit tests. */
  static Evaluation evaluate(List<String> fields, D5Schema.ColumnIndex cols, Set<String> zipRef, int year) {
    Evaluation e = new Evaluation();

    String rawKey = cols.get(fields, D5Schema.UNIQUE_KEY);
    Long key = null;
    if (D5Rules.isMissing(rawKey)) {
      e.rejectReasons.add("MISSING_UNIQUE_KEY");
    } else {
      key = D5Rules.parseUniqueKey(rawKey);
      if (key == null) {
        e.rejectReasons.add("INVALID_UNIQUE_KEY");
      }
    }

    String rawDate = cols.get(fields, D5Schema.CREATED_DATE);
    LocalDateTime created = null;
    if (D5Rules.isMissing(rawDate)) {
      e.rejectReasons.add("MISSING_CREATED_DATE");
    } else {
      created = D5Rules.parseTimestamp(rawDate);
      if (created == null) {
        e.rejectReasons.add("INVALID_CREATED_DATE");
      } else if (created.getYear() != year) {
        e.rejectReasons.add("CREATED_DATE_OUTSIDE_" + year);
      }
    }

    String complaintType = D5Rules.squash(cols.get(fields, D5Schema.COMPLAINT_TYPE));
    if (D5Rules.isMissing(complaintType)) {
      e.rejectReasons.add("MISSING_COMPLAINT_TYPE");
    }
    String descriptor = D5Rules.squash(cols.get(fields, D5Schema.DESCRIPTOR));
    if (D5Rules.isMissing(descriptor)) {
      e.rejectReasons.add("MISSING_DESCRIPTOR");
    }
    if (e.rejected()) {
      return e;
    }

    if (!D5Rules.isAnimalInPark(complaintType)) {
      e.filterReason = "OTHER_COMPLAINT_TYPE";
      return e;
    }
    if (!D5Rules.isDogOffLeash(descriptor)) {
      e.filterReason = "OTHER_DESCRIPTOR";
      return e;
    }

    // ZIP: standardize and match to the D4 spine.
    String rawZip = D5Rules.squash(cols.get(fields, D5Schema.INCIDENT_ZIP));
    D5Rules.ZipStatus zs = D5Rules.zipStatus(rawZip, zipRef);
    String zip = D5Rules.normalizeZip(rawZip);
    if (zs == D5Rules.ZipStatus.MISSING || zs == D5Rules.ZipStatus.INVALID) {
      zip = null;
      e.flags.add("ZIP_" + zs.name());
    } else if (zs == D5Rules.ZipStatus.UNMATCHED) {
      e.flags.add("ZIP_NOT_IN_REFERENCE");
    }
    if (zip != null && D5Rules.isZipPlus4(rawZip)) {
      e.flags.add("ZIP_PLUS4_TRUNCATED");
    }

    String rawBorough = cols.get(fields, D5Schema.BOROUGH);
    String borough = D5Rules.normalizeBorough(rawBorough);
    if (borough == null) {
      e.flags.add("BOROUGH_UNRECOGNIZED");
      borough = D5Rules.squash(rawBorough).toUpperCase();
    } else if (borough.equals(D5Rules.BOROUGH_UNSPECIFIED)) {
      e.flags.add("BOROUGH_UNSPECIFIED");
    }

    String rawStatus = cols.get(fields, D5Schema.STATUS);
    String status;
    if (D5Rules.isMissing(rawStatus)) {
      e.flags.add("STATUS_MISSING");
      status = D5Rules.HIVE_NULL;
    } else {
      status = D5Rules.normalizeStatus(rawStatus);
      if (status == null) {
        e.flags.add("STATUS_UNRECOGNIZED");
        status = D5Rules.squash(rawStatus);
      }
    }

    e.uniqueKey = Long.toString(key);
    e.zipStatus = zs.name();
    String[] out = {
        e.uniqueKey,
        created.format(D5Rules.OUT_TIMESTAMP),
        created.format(D5Rules.OUT_DATE),
        D5Rules.TARGET_COMPLAINT_TYPE,
        D5Rules.TARGET_DESCRIPTOR,
        zip == null ? D5Rules.HIVE_NULL : zip,
        rawZip.isEmpty() ? D5Rules.HIVE_NULL : rawZip,
        zs.name(),
        borough,
        status,
        join(e.flags, ";")};
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < out.length; i++) {
      if (i > 0) {
        sb.append('\t');
      }
      sb.append(D5Schema.tsvSafe(out[i]));
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

  public static class CleanMapper extends Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;
    private D5Schema.ColumnIndex columns;
    private Set<String> zipRef;
    private int year;
    private String source;
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      mos = new MultipleOutputs<Text, Text>(context);
      columns = D5Schema.columns(conf);
      zipRef = ZipReference.load(conf);
      year = conf.getInt(D5Schema.CONF_YEAR, D5Schema.DEFAULT_YEAR);
      source = context.getInputSplit() instanceof FileSplit
          ? ((FileSplit) context.getInputSplit()).getPath().getName() : "input";
    }

    private void side(String output, String reason, long offset, String line) throws IOException, InterruptedException {
      outKey.set(reason);
      outVal.set(source + ":" + offset + "\t" + D5Schema.tsvSafe(line));
      mos.write(output, outKey, outVal, output + "/part");
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
        side("rejected", "MALFORMED_CSV", offset.get(), line);
        return;
      }
      if (columns.isHeader(fields)) {
        context.getCounter(G_INPUT, "HEADER_LINES").increment(1);
        return;
      }
      if (fields.size() != columns.width()) {
        context.getCounter(G_REJECTED, "FIELD_COUNT_MISMATCH").increment(1);
        side("rejected", "FIELD_COUNT_MISMATCH", offset.get(), line);
        return;
      }

      Evaluation e = evaluate(fields, columns, zipRef, year);
      if (e.rejected()) {
        // Counted once under the first reason; all reasons are kept in the output.
        context.getCounter(G_REJECTED, e.rejectReasons.get(0)).increment(1);
        side("rejected", join(e.rejectReasons, ";"), offset.get(), line);
        return;
      }
      if (e.filterReason != null) {
        context.getCounter(G_FILTERED, e.filterReason).increment(1);
        side("filtered", e.filterReason, offset.get(), line);
        return;
      }
      outKey.set(e.uniqueKey);
      outVal.set(e.cleanedTsv + SEP + source + ":" + offset.get() + SEP + line);
      context.write(outKey, outVal);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

  /** Keeps one record per unique_key (earliest created_ts, then raw line) and rejects the rest. */
  public static class DedupReducer extends Reducer<Text, Text, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos;
    private final Text out = new Text();
    private final Text reason = new Text();

    @Override
    protected void setup(Context context) {
      mos = new MultipleOutputs<NullWritable, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      List<String[]> records = new ArrayList<String[]>();
      for (Text v : values) {
        records.add(v.toString().split(SEP, 3)); // cleaned, source, raw
      }
      Collections.sort(records, new Comparator<String[]>() {
        @Override
        public int compare(String[] a, String[] b) {
          int c = a[0].split("\t", 3)[1].compareTo(b[0].split("\t", 3)[1]);
          return c != 0 ? c : a[2].compareTo(b[2]);
        }
      });

      String[] keep = records.get(0);
      out.set(keep[0]);
      mos.write("cleaned", NullWritable.get(), out, "cleaned/part");
      String[] cols = keep[0].split("\t", -1);
      String zipStatus = cols[7];
      context.getCounter(G_RETAINED, "RECORDS").increment(1);
      context.getCounter(G_ZIP, zipStatus).increment(1);
      if (!cols[10].isEmpty()) {
        for (String flag : cols[10].split(";")) {
          context.getCounter(G_FLAGS, flag).increment(1);
        }
      }
      if (!zipStatus.equals(D5Rules.ZipStatus.MATCHED.name())) {
        mos.write("unmatched", NullWritable.get(), out, "unmatched/part");
      }

      for (int i = 1; i < records.size(); i++) {
        String[] dup = records.get(i);
        String r = dup[2].equals(keep[2]) ? "DUPLICATE_ROW" : "DUPLICATE_KEY_CONFLICT";
        context.getCounter(G_REJECTED, r).increment(1);
        reason.set(r);
        out.set(dup[1] + "\t" + D5Schema.tsvSafe(dup[2]));
        mos.write("rejected", reason, out, "rejected/part");
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
    D5Schema.resolveColumns(conf, input);
    conf.set(D5Schema.CONF_ZIP_REF_PATH, zipRef.toString());
    int refSize = ZipReference.load(conf).size(); // fail fast in the driver
    prepareOutput(conf, output);

    Job job = Job.getInstance(conf, "D5 311 clean (Dog Off Leash)");
    job.setJarByClass(CleanJob.class);
    job.setMapperClass(CleanMapper.class);
    job.setReducerClass(DedupReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "cleaned", TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "unmatched", TextOutputFormat.class, NullWritable.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "rejected", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "filtered", TextOutputFormat.class, Text.class, Text.class);
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
    long filtered = sum(c, G_FILTERED);
    long retained = value(c, G_RETAINED, "RECORDS");
    long zipTotal = sum(c, G_ZIP);
    boolean linesOk = lines == blank + header + rejected + filtered + retained;
    boolean zipOk = zipTotal == retained;

    StringBuilder sb = new StringBuilder();
    sb.append("# D5 311 Dog Off Leash cleaning reconciliation\n");
    sb.append("zip_reference_codes\t").append(refSize).append('\n');
    appendGroup(sb, c, G_INPUT);
    appendGroup(sb, c, G_REJECTED);
    appendGroup(sb, c, G_FILTERED);
    appendGroup(sb, c, G_RETAINED);
    appendGroup(sb, c, G_ZIP);
    appendGroup(sb, c, G_FLAGS);
    sb.append(String.format("CHECK\tinput_lines(%d) = blank(%d) + header(%d) + rejected(%d) + filtered(%d) + retained(%d)\t%s%n",
        lines, blank, header, rejected, filtered, retained, linesOk ? "PASS" : "FAIL"));
    sb.append(String.format("CHECK\tretained(%d) = sum(zip_status)(%d)\t%s%n", retained, zipTotal, zipOk ? "PASS" : "FAIL"));
    writeReport(conf, report, sb.toString());
    System.out.print(sb);
    return linesOk && zipOk;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new CleanJob(), args));
  }
}
