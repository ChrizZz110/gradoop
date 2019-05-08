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
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.functions.tpgm.All;
import org.gradoop.flink.model.impl.functions.tpgm.AsOf;
import org.gradoop.flink.model.impl.functions.tpgm.Between;
import org.gradoop.flink.model.impl.functions.tpgm.ContainedIn;
import org.gradoop.flink.model.impl.functions.tpgm.CreatedIn;
import org.gradoop.flink.model.impl.functions.tpgm.DeletedIn;
import org.gradoop.flink.model.impl.functions.tpgm.FromTo;
import org.gradoop.flink.model.impl.functions.tpgm.ValidDuring;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.Snapshot;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized TPGM snapshot benchmark.
 */
public class SnapshotBenchmark extends AbstractRunner implements ProgramDescription {

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
   * Option to declare query from timestamp
   */
  private static final String OPTION_QUERY_FROM = "f";
  /**
   * Option to declare query to timestamp
   */
  private static final String OPTION_QUERY_TO = "t";
  /**
   * Option to declare query type
   */
  private static final String OPTION_QUERY_TYPE = "y";


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
   * Used from timestamp in milliseconds
   */
  private static String QUERY_FROM;
  /**
   * Used to timestamp in milliseconds
   */
  private static String QUERY_TO;
  /**
   * Used query type
   */
  private static String QUERY_TYPE;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH,    "input", true, "Path to indexed source files.");
    OPTIONS.addOption(OPTION_OUTPUT_PATH,   "output", true, "Path to output file");
    OPTIONS.addOption(OPTION_CSV_PATH,      "csv", true, "Path to csv statistics");
    OPTIONS.addOption(OPTION_VERIFICATION,  "verification", false, "Verify Snapshot with join.");
    OPTIONS.addOption(OPTION_QUERY_FROM,    "from", true, "Used query from timestamp [ms]");
    OPTIONS.addOption(OPTION_QUERY_TO,      "to", true, "Used query to timestamp [ms]");
    OPTIONS.addOption(OPTION_QUERY_TYPE,    "type", true, "Used query type");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   * Example: {@code /path/to/flink run -c org.gradoop.benchmark.tpgm.SnapshotBenchmark
   * path/to/gradoop-examples.jar -i hdfs:///graph -o hdfs:///output -c results.csv
   * -f 1287000000000 -y asof}
   *
   * @param args program arguments
   * @throws Exception in case of error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SnapshotBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // read graph
    DataSource source = new CSVDataSource(INPUT_PATH, conf);
    TemporalGraph graph = source.getTemporalGraph();

    // get temporal predicate
    TemporalPredicate temporalPredicate;
    Long queryFrom = Long.valueOf(QUERY_FROM);
    Long queryTo;

    switch (QUERY_TYPE) {
    case "asof" :
      temporalPredicate = new AsOf(queryFrom);
      break;
    case "between" :
      queryTo = Long.valueOf(QUERY_TO);
      temporalPredicate = new Between(queryFrom, queryTo);
      break;
    case "containedin":
      queryTo = Long.valueOf(QUERY_TO);
      temporalPredicate = new ContainedIn(queryFrom, queryTo);
      break;
    case "createdin":
      queryTo = Long.valueOf(QUERY_TO);
      temporalPredicate = new CreatedIn(queryFrom, queryTo);
      break;
    case "deletedin":
      queryTo = Long.valueOf(QUERY_TO);
      temporalPredicate = new DeletedIn(queryFrom, queryTo);
      break;
    case "fromto":
      queryTo = Long.valueOf(QUERY_TO);
      temporalPredicate = new FromTo(queryFrom, queryTo);
      break;
    case "validduring":
      queryTo = Long.valueOf(QUERY_TO);
      temporalPredicate = new ValidDuring(queryFrom, queryTo);
      break;
    case "all":
      default:
        temporalPredicate = new All();
        break;
    }

    // get the snapshot
    TemporalGraph snapshot = graph.callForGraph(new Snapshot(temporalPredicate));

    // apply optional verification
    if (VERIFICATION) {
      snapshot = snapshot.verify();
    }

    // write graph
    DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
    sink.write(snapshot, true);

    // execute and write job statistics
    env.execute(SnapshotBenchmark.class.getSimpleName());
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
    QUERY_FROM   = cmd.getOptionValue(OPTION_QUERY_FROM);
    QUERY_TO     = cmd.getOptionValue(OPTION_QUERY_TO);
    QUERY_TYPE   = cmd.getOptionValue(OPTION_QUERY_TYPE);
    VERIFICATION = cmd.hasOption(OPTION_VERIFICATION);
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File need to be set.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph output directory.");
    }
    if (!cmd.hasOption(OPTION_QUERY_FROM)) {
      throw new IllegalArgumentException("Define a query from timestamp.");
    }
    if (!cmd.hasOption(OPTION_QUERY_TYPE)) {
      throw new IllegalArgumentException("Define a query type, e.g. 'asof'.");
    }
  }

  /**
   * Method to create and add lines to a csv-file
   *
   * @param env given ExecutionEnvironment
   * @throws IOException exeption during file writing
   */
  private static void writeCSV(ExecutionEnvironment env) throws IOException {

    String head = String
      .format("%s|%s|%s|%s|%s|%s|%s%n",
        "Parallelism",
        "dataset",
        "query-type",
        "from(ms)",
        "to(ms)",
        "verify",
        "Runtime(s)");

    String tail = String
      //.format("%s|%s|%s|%s|%s|%s|%s%n",
      .format("%s|%s|%s|%s|%s|%s|%s%n",
        env.getParallelism(),
        INPUT_PATH,
        QUERY_TYPE,
        QUERY_FROM,
        QUERY_TO,
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

  @Override
  public String getDescription() {
    return "TPGM Snapshot operator benchmark.";
  }
}
