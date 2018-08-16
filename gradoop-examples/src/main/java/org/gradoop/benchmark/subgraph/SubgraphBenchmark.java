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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.gradoop.benchmark.cypher.Predicates;
import org.gradoop.examples.AbstractRunner;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.indexed.IndexedCSVDataSource;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.ByLabel;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.gradoop.storage.common.predicate.query.Query;
import org.gradoop.storage.config.GradoopAccumuloConfig;
import org.gradoop.storage.config.GradoopHBaseConfig;
import org.gradoop.storage.impl.accumulo.AccumuloEPGMStore;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSink;
import org.gradoop.storage.impl.accumulo.io.AccumuloDataSource;
import org.gradoop.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.storage.impl.hbase.factory.HBaseEPGMStoreFactory;
import org.gradoop.storage.impl.hbase.io.HBaseDataSink;
import org.gradoop.storage.impl.hbase.io.HBaseDataSource;
import org.gradoop.storage.utils.AccumuloFilters;
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
   * Option to declare path to indexed input graph
   */
  private static final String OPTION_INPUT_PATH = "i";
  /**
   * Option to declare input graph format (indexed, hbase, accumulo)
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
   * Option to declare using predicate pushdown for store input.
   */
  private static final String OPTION_USE_PREDICATE_PUSHDOWN = "p";
  /**
   * Used input path
   */
  private static String INPUT_PATH;
  /**
   * Used input format (indexed, hbase, accumulo)
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
      "Path to indexed source files.");
    OPTIONS.addOption(OPTION_INPUT_FORMAT, "format", true,
      "Input graph format (csv, indexed, hbase, accumulo).");
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
      "Flag to use predicate pushdown for store.");
  }

  /**
   * Main program to run the benchmark. Arguments are the available options.
   *
   * @param args program arguments
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
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    GradoopFlinkConfig conf = GradoopFlinkConfig.createConfig(env);

    // read graph
    DataSource source;
    DataSink sink;
    switch (INPUT_FORMAT) {
    case "indexed":
      source = new IndexedCSVDataSource(INPUT_PATH, conf);
      sink = new CSVDataSink(INPUT_PATH, conf);
      break;
    case "hbase":
      HBaseEPGMStore hBaseEPGMStore = HBaseEPGMStoreFactory.createOrOpenEPGMStore(
        HBaseConfiguration.create(),
        GradoopHBaseConfig.getDefaultConfig(),
        INPUT_PATH + ".");
      source = new HBaseDataSource(hBaseEPGMStore, conf);
      sink = new HBaseDataSink(hBaseEPGMStore, conf);
      // Apply predicate
      if (USE_PREDICATE_PUSHDOWN) {
        source = ((HBaseDataSource) source).applyVertexPredicate(Query.elements().fromAll()
          .where(HBaseFilters.labelIn(VERTEX_LABEL)));
        source = ((HBaseDataSource) source).applyEdgePredicate(Query.elements().fromAll()
          .where(HBaseFilters.labelIn(EDGE_LABEL)));
      }
      break;
    case "accumulo":
      AccumuloEPGMStore accumuloEPGMStore =
        new AccumuloEPGMStore(GradoopAccumuloConfig.getDefaultConfig());
      source = new AccumuloDataSource(accumuloEPGMStore, conf);
      sink = new AccumuloDataSink(accumuloEPGMStore, conf);
      // Apply predicate
      if (USE_PREDICATE_PUSHDOWN) {
        source = ((AccumuloDataSource) source).applyVertexPredicate(Query.elements().fromAll()
          .where(AccumuloFilters.labelIn(VERTEX_LABEL)));
        source = ((AccumuloDataSource) source).applyEdgePredicate(Query.elements().fromAll()
          .where(AccumuloFilters.labelIn(EDGE_LABEL)));
      }
      break;
    default :
      throw new IllegalArgumentException("Unsupported format: " + INPUT_FORMAT);
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
    if (!cmd.hasOption(OPTION_INPUT_PATH)) {
      throw new IllegalArgumentException("Define a graph input directory.");
    }
    if (!cmd.hasOption(OPTION_INPUT_FORMAT)) {
      throw new IllegalArgumentException("Define a input format (indexed, hbase, accumulo).");
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
      .format("%s|%s|%s|%s|%s|%s|%s|%s%n", "Parallelism", "dataset", "format", "vertex-label",
        "edge-label", "verification", "usedPredicatePD", "Runtime(s)");

    String tail = String
      .format("%s|%s|%s|%s|%s|%s|%s|%s%n", env.getParallelism(), INPUT_PATH, INPUT_FORMAT,
        VERTEX_LABEL, EDGE_LABEL, VERIFICATION, USE_PREDICATE_PUSHDOWN,
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
