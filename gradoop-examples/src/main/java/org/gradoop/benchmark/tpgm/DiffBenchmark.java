/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.benchmark.tpgm;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.impl.functions.tpgm.AsOf;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized TPGM graph evolution (diff) benchmark. The temporal
 * predicate function is fixed to AS OF for this benchmark. The two query timestamps are specified
 * during program arguments.
 */
public class DiffBenchmark extends AbstractRunner {
  /**
   * Option to declare path to indexed input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare path to output graph
   */
  private static final String OPTION_OUTPUT_PATH = "o";
  /**
   * Option to declare output path to statistics csv file
   */
  private static final String OPTION_CSV_PATH = "c";
  /**
   * Option to declare verification
   */
  private static final String OPTION_VERIFICATION = "v";
  /**
   * Option to declare the first query timestamp
   */
  private static final String OPTION_QUERY_1 = "x";
  /**
   * Option to declare the second query timestamp
   */
  private static final String OPTION_QUERY_2 = "y";

  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Used output path
   */
  private static String OUTPUT_PATH;
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used verification flag
   */
  private static boolean VERIFICATION;
  /**
   * Used first query timestamp in milliseconds
   */
  private static Long QUERY_FROM_1;
  /**
   * Used second query timestamp in milliseconds
   */
  private static Long QUERY_FROM_2;

  static {
    OPTIONS.addRequiredOption(OPTION_INPUT_PATH, "input", true, "Path to indexed source files.");
    OPTIONS.addRequiredOption(OPTION_OUTPUT_PATH, "output", true, "Path to output file");
    OPTIONS.addRequiredOption(OPTION_CSV_PATH, "csv", true, "Path to csv statistics");
    OPTIONS.addRequiredOption(OPTION_QUERY_1, "queryts1", true, "Used first query timestamp [ms]");
    OPTIONS.addRequiredOption(OPTION_QUERY_2, "queryts2", true, "Used second query timestamp [ms]");
    OPTIONS.addOption(OPTION_VERIFICATION, "verification", false, "Verify Snapshot with join.");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   * Example: {@code /path/to/flink run -c org.gradoop.benchmark.tpgm.SnapshotBenchmark
   * path/to/gradoop-examples.jar -i hdfs:///graph -o hdfs:///output -c results.csv
   * -x 1287000000000 -y 1230000000000}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, DiffBenchmark.class.getName());

    if (cmd == null) {
      return;
    }

    // read cmd arguments
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // read graph
    DataSource source = new CSVDataSource(INPUT_PATH, conf);
    TemporalGraph graph = source.getTemporalGraph();

    // get the diff
    TemporalGraph snapshot = graph.diff(new AsOf(QUERY_FROM_1), new AsOf(QUERY_FROM_2));

    // apply optional verification
    if (VERIFICATION) {
      snapshot = snapshot.verify();
    }

    // write graph
    DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
    sink.write(snapshot, true);

    // execute and write job statistics
    env.execute(DiffBenchmark.class.getSimpleName() + " - P: " + env.getParallelism());
    writeCSV(env);
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH   = cmd.getOptionValue(OPTION_INPUT_PATH);
    OUTPUT_PATH  = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH     = cmd.getOptionValue(OPTION_CSV_PATH);
    QUERY_FROM_1 = Long.valueOf(cmd.getOptionValue(OPTION_QUERY_1));
    QUERY_FROM_2 = Long.valueOf(cmd.getOptionValue(OPTION_QUERY_2));
    VERIFICATION = cmd.hasOption(OPTION_VERIFICATION);
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {
    String head = String
      .format("%s|%s|%s|%s|%s|%s%n",
        "Parallelism",
        "dataset",
        "asOf1(ms)",
        "asOf2(ms)",
        "verify",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s|%s%n",
        env.getParallelism(),
        INPUT_PATH,
        QUERY_FROM_1,
        QUERY_FROM_2,
        VERIFICATION,
        env.getLastJobExecutionResult().getNetRuntime(TimeUnit.SECONDS));

    File f = new File(CSV_PATH);
    if (f.exists() && !f.isDirectory()) {
      FileUtils.writeStringToFile(f, tail, true);
    } else {
      PrintWriter writer = new PrintWriter(CSV_PATH, "UTF-8");
      writer.print(head);
      writer.print(tail);
      writer.close();
    }
  }
}
