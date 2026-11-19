package edu.nyu.pawprint.d5;

import java.io.IOException;

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
 * Job 2: second aggregation over the frequency output of {@link ProfileJob}.
 *
 * <p>Output (tab separated):
 * <pre>
 *   OUT/distinct/part-*    column  distinct_non_missing  missing_rows  values_repeated
 *                          rows_beyond_first  max_frequency
 *   OUT/duplicates/part-*  column  value  count      (unique_key and full-row hash only)
 * </pre>
 * For {@code unique_key}, values_repeated is the number of candidate duplicate keys; for
 * {@code _row_md5} it is the number of distinct lines that appear more than once.
 *
 * Usage: {@code hadoop jar d5-311.jar edu.nyu.pawprint.d5.DistinctJob <profile>/freq <out>}
 */
public class DistinctJob extends D5Tool {

  public static class DistinctMapper extends Mapper<LongWritable, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;
    private final Text outKey = new Text();
    private final Text outVal = new Text();

    @Override
    protected void setup(Context context) {
      mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void map(LongWritable offset, Text value, Context context) throws IOException, InterruptedException {
      String line = value.toString();
      int first = line.indexOf('\t');
      int last = line.lastIndexOf('\t');
      if (first < 0 || last <= first) {
        context.getCounter("D5_DISTINCT", "BAD_FREQ_LINES").increment(1);
        return;
      }
      String column = line.substring(0, first);
      String val = line.substring(first + 1, last);
      long count = Long.parseLong(line.substring(last + 1).trim());
      boolean missing = val.equals(ProfileJob.MISSING_VALUE);
      outKey.set(column);
      outVal.set(count + "\t" + (missing ? "M" : "V"));
      context.write(outKey, outVal);

      boolean keyColumn = column.equals(D5Schema.UNIQUE_KEY) || column.equals(D5Schema.FULL_ROW_HASH);
      if (keyColumn && !missing && count > 1) {
        outKey.set(column + "\t" + val);
        outVal.set(Long.toString(count));
        mos.write("duplicates", outKey, outVal, "duplicates/part");
      }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

  public static class DistinctReducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs<Text, Text> mos;
    private final Text outVal = new Text();

    @Override
    protected void setup(Context context) {
      mos = new MultipleOutputs<Text, Text>(context);
    }

    @Override
    protected void reduce(Text column, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      long distinct = 0;
      long missingRows = 0;
      long repeated = 0;
      long extraRows = 0;
      long maxFreq = 0;
      for (Text t : values) {
        String[] parts = t.toString().split("\t");
        long count = Long.parseLong(parts[0]);
        if (parts[1].equals("M")) {
          missingRows += count;
          continue;
        }
        distinct++;
        maxFreq = Math.max(maxFreq, count);
        if (count > 1) {
          repeated++;
          extraRows += count - 1;
        }
      }
      outVal.set(distinct + "\t" + missingRows + "\t" + repeated + "\t" + extraRows + "\t" + maxFreq);
      mos.write("distinct", column, outVal, "distinct/part");
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
      mos.close();
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: DistinctJob <profile-freq-dir> <output>");
      return 2;
    }
    return runJob(getConf(), new Path(args[0]), new Path(args[1])) ? 0 : 1;
  }

  static boolean runJob(Configuration base, Path freqDir, Path output) throws Exception {
    Configuration conf = new Configuration(base);
    prepareOutput(conf, output);
    Job job = Job.getInstance(conf, "D5 311 profile (distinct values and duplicate keys)");
    job.setJarByClass(DistinctJob.class);
    job.setMapperClass(DistinctMapper.class);
    job.setReducerClass(DistinctReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, freqDir);
    FileOutputFormat.setOutputPath(job, output);
    LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
    MultipleOutputs.addNamedOutput(job, "distinct", TextOutputFormat.class, Text.class, Text.class);
    MultipleOutputs.addNamedOutput(job, "duplicates", TextOutputFormat.class, Text.class, Text.class);
    return job.waitForCompletion(true);
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new DistinctJob(), args));
  }
}
