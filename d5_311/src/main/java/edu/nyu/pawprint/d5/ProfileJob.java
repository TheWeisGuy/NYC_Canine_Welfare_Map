package edu.nyu.pawprint.d5;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * Job 1: column profile of the raw D5 extract.
 *
 * <p>For every analysis column: row count, missing count, inferred-type counts, numeric and
 * timestamp min/max, value length range, and categorical value frequencies. Record-level
 * counts (header, blank, malformed CSV, wrong field count) are profiled under {@code _record}.
 *
 * <p>Output (tab separated):
 * <pre>
 *   OUT/stats/part-*   column  metric  value
 *   OUT/freq/part-*    column  value   count
 * </pre>
 *
 * Usage: {@code hadoop jar d5-311.jar edu.nyu.pawprint.d5.ProfileJob <input> <out>}
 */
public class ProfileJob extends D5Tool {

  static final String STAT = "STAT";
  static final String FREQ = "FREQ";
  static final String RECORD = "_record";
  static final String MISSING_VALUE = "<MISSING>";

  public static class ProfileMapper extends Mapper<LongWritable, Text, Text, Text> {
    private static final Text ONE = new Text("1");
    private final Text outKey = new Text();
    private final Text outVal = new Text();
    private D5Schema.ColumnIndex columns;
    private List<String> profiled;
    private MessageDigest md5;

    @Override
    protected void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      columns = D5Schema.columns(conf);
      profiled = new ArrayList<String>();
      String[] requested = conf.getStrings(D5Schema.CONF_PROFILE_COLUMNS);
      for (String c : requested == null ? D5Schema.REQUIRED_COLUMNS : Arrays.asList(requested)) {
        String n = D5Schema.normalizeName(c);
        if (columns.has(n)) {
          profiled.add(n);
        }
      }
      try {
        md5 = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
    }

    private void emit(String kind, String column, String metric, Text value, Context context)
        throws IOException, InterruptedException {
      outKey.set(kind + "\t" + column + "\t" + D5Schema.tsvSafe(metric));
      context.write(outKey, value);
    }

    private void count(String column, String metric, Context context) throws IOException, InterruptedException {
      emit(STAT, column, metric, ONE, context);
    }

    private void extreme(String column, String metric, String value, Context context)
        throws IOException, InterruptedException {
      outVal.set(value);
      emit(STAT, column, metric, outVal, context);
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      if (offset.get() == 0) {
        line = CsvParser.stripBom(line);
      }
      count(RECORD, "INPUT_LINES", context);
      if (line.trim().isEmpty()) {
        count(RECORD, "BLANK_LINES", context);
        return;
      }
      List<String> fields;
      try {
        fields = D5Schema.split(context.getConfiguration(), line);
      } catch (CsvParser.MalformedCsvException e) {
        count(RECORD, "MALFORMED_CSV", context);
        return;
      }
      if (columns.isHeader(fields)) {
        count(RECORD, "HEADER_LINES", context);
        return;
      }
      if (fields.size() != columns.width()) {
        count(RECORD, "FIELD_COUNT_MISMATCH", context);
        return;
      }
      count(RECORD, "DATA_ROWS", context);
      emit(FREQ, D5Schema.FULL_ROW_HASH, hex(md5.digest(line.getBytes(StandardCharsets.UTF_8))), ONE, context);

      for (String col : profiled) {
        String v = D5Rules.squash(columns.get(fields, col));
        count(col, "ROWS", context);
        if (D5Rules.isMissing(v)) {
          count(col, "MISSING", context);
          emit(FREQ, col, MISSING_VALUE, ONE, context);
          continue;
        }
        count(col, "NON_MISSING", context);
        String type = D5Rules.inferType(v);
        count(col, "TYPE_" + type, context);
        String len = Integer.toString(v.length());
        extreme(col, "MIN_LENGTH", len, context);
        extreme(col, "MAX_LENGTH", len, context);
        if (type.equals("INTEGER") || type.equals("DECIMAL")) {
          extreme(col, "MIN_NUMBER", v, context);
          extreme(col, "MAX_NUMBER", v, context);
        } else if (type.equals("TIMESTAMP")) {
          LocalDateTime ts = D5Rules.parseTimestamp(v);
          String norm = ts.format(D5Rules.OUT_TIMESTAMP);
          extreme(col, "MIN_TIMESTAMP", norm, context);
          extreme(col, "MAX_TIMESTAMP", norm, context);
        }
        emit(FREQ, col, v, ONE, context);
      }
    }
  }

  private static String hex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b & 0xff));
    }
    return sb.toString();
  }

  /** Combines values for one key: sum for counts, min/max for MIN_* and MAX_* metrics. */
  static String combine(String key, Iterable<Text> values) {
    String metric = key.substring(key.lastIndexOf('\t') + 1);
    boolean isStat = key.startsWith(STAT + "\t");
    boolean isMin = isStat && metric.startsWith("MIN_");
    boolean isMax = isStat && metric.startsWith("MAX_");
    if (!isMin && !isMax) {
      long total = 0;
      for (Text v : values) {
        total += Long.parseLong(v.toString());
      }
      return Long.toString(total);
    }
    boolean lexical = metric.endsWith("_TIMESTAMP");
    String best = null;
    BigDecimal bestNum = null;
    for (Text t : values) {
      String v = t.toString();
      if (lexical) {
        if (best == null || (isMin ? v.compareTo(best) < 0 : v.compareTo(best) > 0)) {
          best = v;
        }
      } else {
        BigDecimal n = new BigDecimal(v.startsWith("+") ? v.substring(1) : v);
        if (bestNum == null || (isMin ? n.compareTo(bestNum) < 0 : n.compareTo(bestNum) > 0)) {
          bestNum = n;
          best = v;
        }
      }
    }
    return best;
  }

  public static class ProfileCombiner extends Reducer<Text, Text, Text, Text> {
    private final Text out = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      out.set(combine(key.toString(), values));
      context.write(key, out);
    }
  }

  public static class ProfileReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void setup(Context context) {
      mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      String k = key.toString();
      int tab = k.indexOf('\t');
      String kind = k.substring(0, tab);
      outKey.set(k.substring(tab + 1)); // column \t metric-or-value
      outVal.set(combine(k, values));
      if (kind.equals(STAT)) {
        mos.write("stats", outKey, outVal, "stats/part");
      } else {
        mos.write("freq", outKey, outVal, "freq/part");
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: ProfileJob <input> <output>");
      return 2;
    }
    return runJob(getConf(), new Path(args[0]), new Path(args[1])) ? 0 : 1;
  }

  static boolean runJob(Configuration base, Path input, Path output) throws Exception {
    Configuration conf = new Configuration(base);
    D5Schema.resolveColumns(conf, input);
    prepareOutput(conf, output);

    Job job = Job.getInstance(conf, "D5 311 profile (raw)");
    job.setJarByClass(ProfileJob.class);
    job.setMapperClass(ProfileMapper.class);
    job.setCombinerClass(ProfileCombiner.class);
    job.setReducerClass(ProfileReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "stats", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "freq", TextOutputFormat.class, Text.class, Text.class);
    return job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new ProfileJob(), args));
  }
}
