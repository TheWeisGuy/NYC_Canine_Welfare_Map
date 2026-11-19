package edu.nyu.pawprint.d5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * Runs the full D5 pipeline:
 * <pre>
 *   OUT/profile/raw       ProfileJob   column stats + value frequencies of the raw extract
 *   OUT/profile/distinct  DistinctJob  distinct counts + candidate duplicate keys/rows
 *   OUT/clean             CleanJob     cleaned / rejected / filtered / unmatched + reconciliation
 *   OUT/profile/cleaned   ProfileJob   profile of the cleaned output (post-cleaning check)
 *   OUT/aggregate         AggregateJob Dog Off Leash reports per D4 area code
 * </pre>
 * Usage: {@code hadoop jar d5-311.jar [-D d5.overwrite=true] <input> <zip-reference> <out>}
 */
public class D5Pipeline extends D5Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: D5Pipeline [-D d5.overwrite=true] <input> <zip-reference> <output-base>");
      return 2;
    }
    Configuration conf = getConf();
    Path input = new Path(args[0]);
    Path zipRef = new Path(args[1]);
    Path out = new Path(args[2]);
    Path rawProfile = new Path(out, "profile/raw");
    Path clean = new Path(out, "clean");

    if (!ProfileJob.runJob(conf, input, rawProfile)) {
      return fail("ProfileJob");
    }
    if (!DistinctJob.runJob(conf, new Path(rawProfile, "freq"), new Path(out, "profile/distinct"))) {
      return fail("DistinctJob");
    }
    if (!CleanJob.runJob(conf, input, zipRef, clean)) {
      return fail("CleanJob (job failed or reconciliation check failed; see " + clean + "/reconciliation.txt)");
    }
    // Profile the cleaned rows with the same job, using the cleaned schema.
    Configuration cleanedConf = new Configuration(conf);
    cleanedConf.set(D5Schema.CONF_INPUT_FORMAT, "tsv");
    cleanedConf.setStrings(D5Schema.CONF_COLUMNS, CleanJob.CLEANED_COLUMNS);
    cleanedConf.set(D5Schema.CONF_PROFILE_COLUMNS,
        "unique_key,created_ts,complaint_type,descriptor,incident_zip,zip_status,borough,status,quality_flags");
    if (!ProfileJob.runJob(cleanedConf, new Path(clean, "cleaned"), new Path(out, "profile/cleaned"))) {
      return fail("ProfileJob (cleaned)");
    }
    if (!AggregateJob.runJob(conf, new Path(clean, "cleaned"), zipRef, new Path(out, "aggregate"))) {
      return fail("AggregateJob (job failed or reconciliation check failed)");
    }
    System.out.println("D5 pipeline complete: " + out);
    return 0;
  }

  private static int fail(String stage) {
    System.err.println("D5 pipeline stopped at " + stage);
    return 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new D5Pipeline(), args));
  }
}
