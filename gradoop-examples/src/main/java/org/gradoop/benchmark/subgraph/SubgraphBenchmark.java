/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.benchmark.subgraph;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;
import org.gradoop.storage.utils.HBaseFilters;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.TimeUnit;

/**
 * A dedicated program for parametrized subgraph benchmark.
 */
public class SubgraphBenchmark extends AbstractRunner implements ProgramDescription {
  /**
   * Option to declare path to input graph. Using a store, the path is the table prefix without '.'.
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare input graph format (csv, indexed, hbase)
   */
  private static final String OPTION_INPUT_FORMAT = "f";
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
   * Option to declare used vertex label
   */
  private static final String OPTION_VERTEX_LABEL = "vl";
  /**
   * Option to declare used edge label
   */
  private static final String OPTION_EDGE_LABEL = "el";
  /**
   * Option to declare using predicate pushdown for graph store
   */
  private static final String OPTION_USE_PREDICATE_PUSHDOWN = "r";
  /**
   * Used input path or table prefix (HBase)
   */
  private static String INPUT_PATH;
  /**
   * Used input format (csv, indexed, hbase)
   */
  private static String INPUT_FORMAT;
  /**
   * Used output path
   */
  private static String OUTPUT_PATH;
  /**
   * Used csv path
   */
  private static String CSV_PATH;
  /**
   * Used vertex label
   */
  private static String VERTEX_LABEL;
  /**
   * Used edge label
   */
  private static String EDGE_LABEL;
  /**
   * Used verification flag
   */
  private static boolean VERIFICATION;
  /**
   * Used to indicate the usage of predicate pushdown
   */
  private static boolean USE_PREDICATE_PUSHDOWN;

  static {
    OPTIONS.addOption(OPTION_INPUT_PATH, "input", true,
      "Path to csv source files or table prefix (if a store is chosen as format).");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "format", true,
      "Input graph format (csv, indexed, hbase).");
    OPTIONS.addOption(OPTION_OUTPUT_PATH, "output", true,
      "Path to output file");
    OPTIONS.addOption(OPTION_CSV_PATH, "csv", true,
      "Path to csv statistics");
    OPTIONS.addOption(OPTION_VERIFICATION, "verification", false,
      "Verify Subgraph with join.");
    OPTIONS.addOption(OPTION_VERTEX_LABEL, "vertex-label", true,
      "Used vertex label");
    OPTIONS.addOption(OPTION_EDGE_LABEL, "edge-label", true,
      "Used edge label");
    OPTIONS.addOption(OPTION_USE_PREDICATE_PUSHDOWN, "predicatepushdown", false,
      "Flag to enable predicate pushdown for store.");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments, e.g.:
   *             -i hdfs:///mygraph -f indexed -o hdfs:///output -c /home/user/result.csv -v
   *             -vl person -el knows
   * @throws Exception in case of Error
   */
  public static void main(String[] args) throws Exception {
    CommandLine cmd = parseArguments(args, SubgraphBenchmark.class.getName());

    if (cmd == null) {
      System.exit(1);
    }

    // test if minimum arguments are set
    performSanityCheck(cmd);

    // read cmd arguments
    readCMDArguments(cmd);

    // create gradoop config
    ExecutionEnvironment env = getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // read graph
    DataSource source = getDataSource(INPUT_PATH, INPUT_FORMAT, conf);

    // apply predicates to store sources if pushdown is enabled
    if (USE_PREDICATE_PUSHDOWN) {
      switch (INPUT_FORMAT) {
      case FORMAT_HBASE:
        source = ((HBaseDataSource) source).applyVertexPredicate(
          Query.elements().fromAll().where(HBaseFilters.labelIn(VERTEX_LABEL)));
        source = ((HBaseDataSource) source).applyEdgePredicate(
          Query.elements().fromAll().where(HBaseFilters.labelIn(EDGE_LABEL)));
        break;
      default:
        throw new IllegalArgumentException("The flag to enable predicate pushdown is only valid" +
          " with the input formats: [hbase,accumulo].");
      }
    }

    LogicalGraph graph = source.getLogicalGraph();

    // compute subgraph -> verify results (join) vs no verify (filter)
    if (VERIFICATION) {
      graph = graph.subgraph(new ByLabel<>(VERTEX_LABEL), new ByLabel<>(EDGE_LABEL),
        Subgraph.Strategy.BOTH_VERIFIED);
    } else {
      graph = graph.subgraph(new ByLabel<>(VERTEX_LABEL), new ByLabel<>(EDGE_LABEL),
        Subgraph.Strategy.BOTH);
    }

    // write graph
    DataSink sink = new CSVDataSink(OUTPUT_PATH, conf);
    sink.write(graph);

    // execute and write job statistics
    env.execute();
    writeCSV(env);
  }

  /**
   * Reads the given arguments from command line
   *
   * @param cmd command line
   */
  private static void readCMDArguments(CommandLine cmd) {
    INPUT_PATH   = cmd.getOptionValue(OPTION_INPUT_PATH);
    INPUT_FORMAT = cmd.getOptionValue(OPTION_INPUT_FORMAT);
    OUTPUT_PATH  = cmd.getOptionValue(OPTION_OUTPUT_PATH);
    CSV_PATH     = cmd.getOptionValue(OPTION_CSV_PATH);
    VERTEX_LABEL = cmd.getOptionValue(OPTION_VERTEX_LABEL);
    EDGE_LABEL   = cmd.getOptionValue(OPTION_EDGE_LABEL);
    VERIFICATION = cmd.hasOption(OPTION_VERIFICATION);
    USE_PREDICATE_PUSHDOWN = cmd.hasOption(OPTION_USE_PREDICATE_PUSHDOWN);
  }

  /**
   * Checks if the minimum of arguments is provided
   *
   * @param cmd command line
   */
  private static void performSanityCheck(CommandLine cmd) {
    if (!cmd.hasOption(OPTION_INPUT_FORMAT)) {
      throw new IllegalArgumentException("Define a graph input format.");
    }
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_CSV_PATH)) {
      throw new IllegalArgumentException("Path to CSV-File need to be set.");
    }
    if (!cmd.hasOption(OPTION_OUTPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph output directory.");
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
      .format("%s|%s|%s|%s|%s|%s|%s|%s%n",
        "Parallelism",
        "dataset",
        "format",
        "vertex-label",
        "edge-label",
        "verification",
        "predPushdown",
        "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s|%s|%s|%s%n",
        env.getParallelism(),
        INPUT_PATH,
        INPUT_FORMAT,
        VERTEX_LABEL,
        EDGE_LABEL,
        VERIFICATION,
        USE_PREDICATE_PUSHDOWN,
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

  /**
   * {@inheritDoc}
   */
  @Override
  public String getDescription() {
    return SubgraphBenchmark.class.getName();
  }
}
