package edu.nyu.pawprint.d1;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * Job 4: license periods per D4 area code for one extract year (default 2026).
 *
 * <p>Reads {@link CleanJob}'s cleaned output and counts MATCHED records of the selected extract
 * per ZIP. As in D5's AggregateJob, every code in the D4 reference gets a row, with explicit
 * zeros where no records matched, so zeros and missing values stay distinct in Hive joins.
 *
 * <p>Distinct rows are counted with a secondary sort instead of in-memory sets: the map key
 * is {@code zip \t md5(cleaned row)}, records are partitioned and grouped by ZIP only, and the
 * reducer counts changes of the row hash within each ZIP.
 *
 * <p>Output {@code OUT/by_zip/part-*} (tab separated):
 * <pre>
 *   zip  license_periods  distinct_license_rows  identical_repeat_rows
 * </pre>
 * License periods are registration activity, not unique dogs.
 *
 * Usage: {@code hadoop jar d1-dog-licensing.jar edu.nyu.pawprint.d1.AggregateJob [-D d1.extract.year=2026]
 *        <clean>/cleaned <zip-ref> <out>}
 */
public class AggregateJob extends D1Tool {

  static final String G_AGG = "D1_AGGREGATE";
  static final String G_YEAR_ZIP = "D1_SELECTED_YEAR_ZIP_STATUS";

  static String zipOf(String key) {
    int tab = key.indexOf('\t');
    return tab < 0 ? key : key.substring(0, tab);
  }

  /** Same partition for a ZIP whether it comes from a map key or the reference list. */
  static int partitionOf(String zip, int partitions) {
    return (zip.hashCode() & Integer.MAX_VALUE) % partitions;
  }

  public static class ZipMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private static final LongWritable ONE = new LongWritable(1);
    private final Text outKey = new Text();
    private String year;
    private MessageDigest md5;

    @Override
    protected void setup(Context context) throws IOException {
      year = Integer.toString(context.getConfiguration().getInt(
          D1Schema.CONF_EXTRACT_YEAR, D1Schema.DEFAULT_EXTRACT_YEAR));
      try {
        md5 = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IOException(e);
      }
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      String[] c = line.split("\t", -1);
      if (c.length != CleanJob.CLEANED_COLUMNS.length) {
        context.getCounter(G_AGG, "BAD_CLEANED_LINES").increment(1);
        return;
      }
      context.getCounter(G_AGG, "CLEANED_RECORDS").increment(1);
      if (!c[CleanJob.C_YEAR].equals(year)) {
        context.getCounter(G_AGG, "SKIPPED_OTHER_EXTRACT_YEAR").increment(1);
        return;
      }
      context.getCounter(G_YEAR_ZIP, c[CleanJob.C_ZIP_STATUS]).increment(1);
      if (!c[CleanJob.C_ZIP_STATUS].equals(D1Rules.ZipStatus.MATCHED.name())) {
        context.getCounter(G_AGG, "SKIPPED_NOT_MATCHED").increment(1);
        return;
      }
      context.getCounter(G_AGG, "MATCHED_RECORDS").increment(1);
      // Row identity = every cleaned column except the derived quality flags.
      String row = line.substring(0, line.lastIndexOf('\t'));
      byte[] d = md5.digest(row.getBytes(StandardCharsets.UTF_8));
      StringBuilder sb = new StringBuilder(c[CleanJob.C_ZIP]).append('\t');
      for (byte b : d) {
        sb.append(String.format("%02x", b & 0xff));
      }
      outKey.set(sb.toString());
      context.write(outKey, ONE);
    }
  }

  /** Sums counts of identical (zip, row hash) keys; also used as combiner. */
  public static class SumCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    private final LongWritable out = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long s = 0;
      for (LongWritable v : values) {
        s += v.get();
      }
      out.set(s);
      context.write(key, out);
    }
  }

  /** Partitions by the ZIP part of the key only. */
  public static class ZipPartitioner extends Partitioner<Text, LongWritable> {
    @Override
    public int getPartition(Text key, LongWritable value, int numPartitions) {
      return partitionOf(zipOf(key.toString()), numPartitions);
    }
  }

  /** Groups reducer input by ZIP only; keys still arrive sorted by (zip, row hash). */
  public static class ZipGroupingComparator extends WritableComparator {
    public ZipGroupingComparator() {
      super(Text.class, true);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {
      return zipOf(a.toString()).compareTo(zipOf(b.toString()));
    }
  }

  public static class ZipReducer extends Reducer<Text, LongWritable, NullWritable, Text> {
    private MultipleOutputs<NullWritable, Text> mos;
    private Set<String> reference;
    private final Set<String> seen = new HashSet<String>();
    private final Text out = new Text();

    @Override
    protected void setup(Context context) throws IOException {
      mos = new MultipleOutputs<NullWritable, Text>(context);
      reference = ZipReference.load(context.getConfiguration());
    }

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      String zip = zipOf(key.toString());
      long periods = 0;
      long distinct = 0;
      String lastHash = null;
      for (LongWritable v : values) {
        String hash = key.toString().substring(zip.length() + 1); // key advances with values
        if (!hash.equals(lastHash)) {
          distinct++;
          lastHash = hash;
        }
        periods += v.get();
      }
      seen.add(zip);
      if (!reference.contains(zip)) {
        context.getCounter(G_AGG, "ZIP_NOT_IN_REFERENCE").increment(1); // should never happen
      }
      context.getCounter(G_AGG, "PERIODS_AGGREGATED").increment(periods);
      context.getCounter(G_AGG, "DISTINCT_ROWS_AGGREGATED").increment(distinct);
      context.getCounter(G_AGG, "ZIPS_WITH_RECORDS").increment(1);
      out.set(zip + "\t" + periods + "\t" + distinct + "\t" + (periods - distinct));
      mos.write("byzip", NullWritable.get(), out, "by_zip/part");
    }

    /** Emits zero rows for reference codes routed to this reducer that had no records. */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      int partitions = context.getNumReduceTasks();
      int mine = context.getTaskAttemptID().getTaskID().getId();
      for (String code : reference) {
        if (partitionOf(code, partitions) == mine && !seen.contains(code)) {
          context.getCounter(G_AGG, "ZIPS_WITH_ZERO_RECORDS").increment(1);
          out.set(code + "\t0\t0\t0");
          mos.write("byzip", NullWritable.get(), out, "by_zip/part");
        }
      }
      mos.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: AggregateJob [-D d1.extract.year=2026] <cleaned-dir> <zip-reference> <output>");
      return 2;
    }
    return runJob(getConf(), new Path(args[0]), new Path(args[1]), new Path(args[2])) ? 0 : 1;
  }

  static boolean runJob(Configuration base, Path cleaned, Path zipRef, Path output) throws Exception {
    Configuration conf = new Configuration(base);
    conf.set(D1Schema.CONF_ZIP_REF_PATH, zipRef.toString());
    int refSize = ZipReference.load(conf).size();
    int year = conf.getInt(D1Schema.CONF_EXTRACT_YEAR, D1Schema.DEFAULT_EXTRACT_YEAR);
    prepareOutput(conf, output);

    Job job = Job.getInstance(conf, "D1 dog license periods by ZIP (extract " + year + ")");
    job.setJarByClass(AggregateJob.class);
    job.setMapperClass(ZipMapper.class);
    job.setCombinerClass(SumCombiner.class);
    job.setPartitionerClass(ZipPartitioner.class);
    job.setGroupingComparatorClass(ZipGroupingComparator.class);
    job.setReducerClass(ZipReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(NullWritable.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, cleaned);
    FileOutputFormat.setOutputPath(job, output);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "byzip", TextOutputFormat.class, NullWritable.class, Text.class);
    if (!job.waitForCompletion(true)) {
      return false;
    }

    Counters c = job.getCounters();
    long matched = value(c, G_AGG, "MATCHED_RECORDS");
    long aggregated = value(c, G_AGG, "PERIODS_AGGREGATED");
    long withRecords = value(c, G_AGG, "ZIPS_WITH_RECORDS");
    long zero = value(c, G_AGG, "ZIPS_WITH_ZERO_RECORDS");
    long outside = value(c, G_AGG, "ZIP_NOT_IN_REFERENCE");
    boolean periodsOk = matched == aggregated;
    boolean spineOk = withRecords + zero == refSize && outside == 0;
    StringBuilder sb = new StringBuilder("# D1 dog licensing aggregation reconciliation (extract_year " + year + ")\n");
    appendGroup(sb, c, G_AGG);
    appendGroup(sb, c, G_YEAR_ZIP);
    sb.append(String.format("CHECK\tmatched_records(%d) = periods_aggregated(%d)\t%s%n",
        matched, aggregated, periodsOk ? "PASS" : "FAIL"));
    sb.append(String.format("CHECK\tzips_with_records(%d) + zero_rows(%d) = reference_codes(%d)\t%s%n",
        withRecords, zero, refSize, spineOk ? "PASS" : "FAIL"));
    writeReport(conf, new Path(output, "reconciliation.txt"), sb.toString());
    System.out.print(sb);
    return periodsOk && spineOk;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new AggregateJob(), args));
  }
}
