package edu.nyu.pawprint.d5;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.util.Tool;

/** Shared driver helpers. */
abstract class D5Tool extends Configured implements Tool {

  /** Deletes {@code out} when -D d5.overwrite=true; otherwise Hadoop fails if it exists. */
  static void prepareOutput(Configuration conf, Path out) throws IOException {
    FileSystem fs = out.getFileSystem(conf);
    if (conf.getBoolean(D5Schema.CONF_OVERWRITE, false) && fs.exists(out)) {
      fs.delete(out, true);
    }
  }

  static long sum(Counters counters, String group) {
    long total = 0;
    CounterGroup g = counters.getGroup(group);
    for (Counter c : g) {
      total += c.getValue();
    }
    return total;
  }

  static long value(Counters counters, String group, String name) {
    return counters.findCounter(group, name).getValue();
  }

  /** Writes a small text report (e.g. reconciliation counts) next to job output. */
  static void writeReport(Configuration conf, Path file, String text) throws IOException {
    FileSystem fs = file.getFileSystem(conf);
    try (PrintWriter w = new PrintWriter(new OutputStreamWriter(fs.create(file, true), StandardCharsets.UTF_8))) {
      w.print(text);
    }
  }

  static void appendGroup(StringBuilder sb, Counters counters, String group) {
    for (Counter c : counters.getGroup(group)) {
      sb.append(group).append('\t').append(c.getName()).append('\t').append(c.getValue()).append('\n');
    }
  }
}
