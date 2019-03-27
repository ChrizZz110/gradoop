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
package org.gradoop.flink.model.impl.operators.tpgm.snapshot;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.tpgm.All;
import org.gradoop.flink.model.impl.functions.tpgm.AsOf;
import org.gradoop.flink.model.impl.functions.tpgm.Between;
import org.gradoop.flink.model.impl.functions.tpgm.ContainedIn;
import org.gradoop.flink.model.impl.functions.tpgm.CreatedIn;
import org.gradoop.flink.model.impl.functions.tpgm.DeletedIn;
import org.gradoop.flink.model.impl.functions.tpgm.FromTo;
import org.gradoop.flink.model.impl.functions.tpgm.ValidDuring;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.functions.Extractor;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

/**
 * Test for the snapshot operator for temporal graphs.
 */
@RunWith(Parameterized.class)
public class SnapshotTest extends GradoopFlinkTestBase {

  /**
   * The temporal predicate to test.
   */
  @Parameterized.Parameter
  public TemporalPredicate predicate;

  /**
   * The variable name of the input graph.
   */
  @Parameterized.Parameter(1)
  public String inputGraph;

  /**
   * The expected result graph for the ascii graph loader.
   */
  @Parameterized.Parameter(2)
  public String resultGraph;

  /**
   * Run the test. Calls the snapshot operator using a predicate and compares results with the
   * expected result graph.
   *
   * @throws Exception when the Execution in Flink fails.
   */
  @Test
  public void runTest() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();
    loader.appendToDatabaseFromString(resultGraph);
    TemporalGraph input = toTemporalGraph(loader.getLogicalGraphByVariable(inputGraph));
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    TemporalGraph result = input.callForGraph(new Snapshot(predicate));
    collectAndAssertTrue(result.toLogicalGraph().equalsByElementData(expected));
  }

  /**
   * Helper function creating a temporal graph from a logical graph.
   * Temporal data is extracted from properties.
   *
   * @param logicalGraph The logical graph.
   * @return The temporal graph.
   */
  private TemporalGraph toTemporalGraph(LogicalGraph logicalGraph) {
    return getConfig().getTemporalGraphFactory()
      .fromNonTemporalDataSets(
        logicalGraph.getGraphHead(),
        new Extractor<>(),
        logicalGraph.getVertices(),
        new Extractor<>(),
        logicalGraph.getEdges(),
        new Extractor<>());
  }

  /**
   * Parameters for this parametrized test.
   *
   * @return An array containing arrays in the form of {@code {predicate, inputGraph,
   * extraGraphData}}.
   */
  @Parameterized.Parameters(name = "{1} {0}")
  public static Iterable<Object[]> parameters() {
    // Another test graph with more validTo times.
    final String g4 = "g4[(t1:A {__valFrom: 1543700000000L, __valTo: 1543900000000L})-->" +
      "(t2:B {__valFrom: 1543700000000L})-->(t3:C {__valTo: 1543800000000L})-->" +
      "(t4:D {__valFrom: 1543600000000L, __valTo: 1543800000000L})-->" +
      "(t5:F {__valFrom: 1543700000000L, __valTo: 1544000000000L})]";
    return Arrays.asList(new Object[][] {
      {new All(), "g0",
        "expected[(eve)-[eka]->(alice)-[akb]->(bob)(eve)-[ekb]->(bob)-[bka]->(alice)]"},
      {new AsOf(1543600000000L), "g0", "expected[(alice)-[akb]->(bob)-[bka]->(alice)]"},
      {new Between(1543500000000L, 1543800000000L), "g0",
        "expected[(eve)-[eka]->(alice)-[akb]->(bob)-[bka]->(alice)]"},
      {new ContainedIn(1543700000000L, 1543900000000L), "g4", g4 + "expected[(t1)]"},
      {new CreatedIn(1543500000000L, 1543800000000L), "g0", "expected[(eve)(bob)]"},
      {new DeletedIn(1543900000000L, 1543900000000L), "g4", g4 + "expected[(t1)]"},
      {new FromTo(1543500000000L, 1543800000000L), "g0",
        "expected[(alice)-[akb]->(bob)-[bka]->(alice)]"},
      {new ValidDuring(1543600000000L, 1543800000000L), "g3",
        "expected[(gps)-[:hasMember]->(carol)]"}
    });
  }
}
