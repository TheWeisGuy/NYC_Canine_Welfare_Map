package edu.nyu.pawprint.d1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.ToolRunner;

/**
 * Runs the full D1 pipeline (same stage layout as D5Pipeline):
 * <pre>
 *   OUT/profile/raw       ProfileJob   column stats + value frequencies of the raw export
 *   OUT/profile/distinct  DistinctJob  distinct counts + identical rows / candidate dog keys
 *   OUT/clean             CleanJob     cleaned / rejected / unmatched + reconciliation
 *   OUT/profile/cleaned   ProfileJob   profile of the cleaned output (post-cleaning check)
 *   OUT/aggregate         AggregateJob license periods per D4 area code (extract d1.extract.year)
 * </pre>
 * Usage: {@code hadoop jar d1-dog-licensing.jar [-D d1.overwrite=true] [-D d1.extract.year=2026]
 *        <input> <zip-reference> <out>}
 */
public class D1Pipeline extends D1Tool {

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: D1Pipeline [-D d1.overwrite=true] [-D d1.extract.year=2026] <input> <zip-reference> <output-base>");
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
    cleanedConf.set(D1Schema.CONF_INPUT_FORMAT, "tsv");
    cleanedConf.setStrings(D1Schema.CONF_COLUMNS, CleanJob.CLEANED_COLUMNS);
    cleanedConf.set(D1Schema.CONF_PROFILE_COLUMNS,
        "extract_year,license_issued_date,license_expired_date,license_days,owner_zip,zip_status,"
            + "animal_birth_year,animal_gender,breed_name,quality_flags");
    if (!ProfileJob.runJob(cleanedConf, new Path(clean, "cleaned"), new Path(out, "profile/cleaned"))) {
      return fail("ProfileJob (cleaned)");
    }
    if (!AggregateJob.runJob(conf, new Path(clean, "cleaned"), zipRef, new Path(out, "aggregate"))) {
      return fail("AggregateJob (job failed or reconciliation check failed)");
    }
    System.out.println("D1 pipeline complete: " + out);
    return 0;
  }

  private static int fail(String stage) {
    System.err.println("D1 pipeline stopped at " + stage);
    return 1;
  }

  public static void main(String[] args) throws Exception {
    System.exit(ToolRunner.run(new Configuration(), new D1Pipeline(), args));
  }
}
