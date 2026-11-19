package edu.nyu.pawprint.d5;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.ToolRunner;

/**
 * Job 4: 2025 Dog Off Leash reports per D4 area code.
 *
 * <p>Reads {@code CleanJob}'s cleaned output and counts MATCHED reports per ZIP. Every code in
 * the D4 reference gets a row, with an explicit 0 where no reports were matched, so zeros
 * and missing values are distinguished when Hive joins this onto the area spine.
 *
 * <p>Output {@code OUT/by_zip/part-*} (tab separated):
 * <pre>
 *   zip  dog_off_leash_reports  closed_reports  other_status_reports
 *   first_report_date  last_report_date  boroughs
 * </pre>
 * Zero-report rows use \N for dates and boroughs. These are reported events, not unique
 * incidents or a measure of unmet demand.
 *
 * Usage: {@code hadoop jar d5-311.jar edu.nyu.pawprint.d5.AggregateJob <clean>/cleaned <zip-ref> <out>}
 */
public class AggregateJob extends D5Tool {

  static final String G_AGG = "D5_AGGREGATE";

  public static class ZipMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text zip = new Text();
    private final Text val = new Text();

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
      String[] c = value.toString().split("\t", -1);
      if (c.length != CleanJob.CLEANED_COLUMNS.length) {
        context.getCounter(G_AGG, "BAD_CLEANED_LINES").increment(1);
        return;
      }
      context.getCounter(G_AGG, "CLEANED_RECORDS").increment(1);
      if (!c[7].equals(D5Rules.ZipStatus.MATCHED.name())) {
        context.getCounter(G_AGG, "SKIPPED_NOT_MATCHED").increment(1);
        return;
      }
      context.getCounter(G_AGG, "MATCHED_RECORDS").increment(1);
      zip.set(c[5]);
      val.set(c[2] + "\t" + c[9] + "\t" + c[8]); // created_date, status, borough
      context.write(zip, val);
    }
  }

  public static class ZipReducer extends Reducer<Text, Text, NullWritable, Text> {
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
    protected void reduce(Text zip, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      long total = 0;
      long closed = 0;
      String first = null;
      String last = null;
      Set<String> boroughs = new TreeSet<String>();
      for (Text t : values) {
        String[] v = t.toString().split("\t", -1);
        total++;
        if (v[1].equals("Closed")) {
          closed++;
        }
        if (first == null || v[0].compareTo(first) < 0) {
          first = v[0];
        }
        if (last == null || v[0].compareTo(last) > 0) {
          last = v[0];
        }
        boroughs.add(v[2]);
      }
      seen.add(zip.toString());
      if (!reference.contains(zip.toString())) {
        context.getCounter(G_AGG, "ZIP_NOT_IN_REFERENCE").increment(1); // should never happen
      }
      context.getCounter(G_AGG, "REPORTS_AGGREGATED").increment(total);
      context.getCounter(G_AGG, "ZIPS_WITH_REPORTS").increment(1);
      out.set(zip + "\t" + total + "\t" + closed + "\t" + (total - closed) + "\t" + first + "\t" + last
          + "\t" + CleanJob.join(new java.util.ArrayList<String>(boroughs), ";"));
      mos.write("byzip", NullWritable.get(), out, "by_zip/part");
    }

    /** Emits zero rows for reference codes routed to this reducer that had no reports. */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      int partitions = context.getNumReduceTasks();
      int mine = context.getTaskAttemptID().getTaskID().getId();
      HashPartitioner<Text, Text> partitioner = new HashPartitioner<Text, Text>();
      Text zip = new Text();
      for (String code : reference) {
        zip.set(code);
        if (partitioner.getPartition(zip, null, partitions) == mine && !seen.contains(code)) {
          context.getCounter(G_AGG, "ZIPS_WITH_ZERO_REPORTS").increment(1);
          out.set(code + "\t0\t0\t0\t" + D5Rules.HIVE_NULL + "\t" + D5Rules.HIVE_NULL + "\t" + D5Rules.HIVE_NULL);
          mos.write("byzip", NullWritable.get(), out, "by_zip/part");
        }
      }
      mos.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: AggregateJob <cleaned-dir> <zip-reference> <output>");
      return 2;
    }
    return runJob(getConf(), new Path(args[0]), new Path(args[1]), new Path(args[2])) ? 0 : 1;
  }

  static boolean runJob(Configuration base, Path cleaned, Path zipRef, Path output) throws Exception {
    Configuration conf = new Configuration(base);
    conf.set(D5Schema.CONF_ZIP_REF_PATH, zipRef.toString());
    int refSize = ZipReference.load(conf).size();
    prepareOutput(conf, output);

    Job job = Job.getInstance(conf, "D5 311 Dog Off Leash reports by ZIP");
    job.setJarByClass(AggregateJob.class);
    job.setMapperClass(ZipMapper.class);
    job.setReducerClass(ZipReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
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
    long aggregated = value(c, G_AGG, "REPORTS_AGGREGATED");
    long withReports = value(c, G_AGG, "ZIPS_WITH_REPORTS");
    long zero = value(c, G_AGG, "ZIPS_WITH_ZERO_REPORTS");
    long outside = value(c, G_AGG, "ZIP_NOT_IN_REFERENCE");
    boolean reportsOk = matched == aggregated;
    boolean spineOk = withReports + zero == refSize && outside == 0;
    StringBuilder sb = new StringBuilder("# D5 311 aggregation reconciliation\n");
    appendGroup(sb, c, G_AGG);
    sb.append(String.format("CHECK\tmatched_records(%d) = reports_aggregated(%d)\t%s%n",
        matched, aggregated, reportsOk ? "PASS" : "FAIL"));
    sb.append(String.format("CHECK\tzips_with_reports(%d) + zero_rows(%d) = reference_codes(%d)\t%s%n",
        withReports, zero, refSize, spineOk ? "PASS" : "FAIL"));
    writeReport(conf, new Path(output, "reconciliation.txt"), sb.toString());
    System.out.print(sb);
    return reportsOk && spineOk;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new AggregateJob(), args));
  }
}
