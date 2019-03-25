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
package org.gradoop.flink.model.impl.tpgm;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.csv.CSVDataSink;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.dot.DOTDataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * Test class of {@link TemporalGraph}.
 */
public class TemporalGraphTest extends GradoopFlinkTestBase {

  /**
   * Temporal graph to test
   */
  private TemporalGraph testGraph;

  /**
   * Logical graph to test
   */
  private LogicalGraph testLogicalGraph;

  /**
   * Temporary test folder to write the test graph.
   */
  @Rule
  public TemporaryFolder testFolder = new TemporaryFolder();

  /**
   * Creates a test temporal graph from the social network loader
   *
   * @throws Exception if loading the graph fails
   */
  @Before
  public void setUp() throws Exception {
    testLogicalGraph = getSocialNetworkLoader().getLogicalGraph();
    testGraph = testLogicalGraph.toTemporalGraph();
  }

  /**
   * Test the {@link TemporalGraph#getConfig()} method.
   */
  @Test
  public void testGetConfig() {
    assertNotNull(testGraph.getConfig());
    assertTrue(testGraph.getConfig() instanceof GradoopFlinkConfig);
  }

  /**
   * Test the {@link TemporalGraph#isEmpty()} method.
   */
  @Test
  public void testIsEmpty() throws Exception {
    collectAndAssertFalse(testGraph.isEmpty());
  }

  /**
   * Test the {@link TemporalGraph#writeTo(DataSink)} method.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testWriteTo() throws Exception {
    String tempFolderPath = testFolder.newFolder().getPath();

    testGraph.writeTo(new CSVDataSink(tempFolderPath, getConfig()));
    getExecutionEnvironment().execute();

    DataSource dataSource = new CSVDataSource(tempFolderPath, getConfig());

    collectAndAssertTrue(dataSource
      .getTemporalGraph()
      .toLogicalGraph()
      .equalsByElementData(testGraph.toLogicalGraph()));
  }

  /**
   * Test the {@link TemporalGraph#writeTo(DataSink, boolean)} method.
   *
   * @throws Exception in case of failure
   */
  @Test
  public void testWriteToOverwrite() throws Exception {
    String tempFolderPath = testFolder.newFolder().getPath();

    testGraph.writeTo(new CSVDataSink(tempFolderPath, getConfig()));
    getExecutionEnvironment().execute();

    testGraph.writeTo(new CSVDataSink(tempFolderPath, getConfig()), true);
    getExecutionEnvironment().execute();

    DataSource dataSource = new CSVDataSource(tempFolderPath, getConfig());

    collectAndAssertTrue(dataSource
      .getTemporalGraph()
      .toLogicalGraph()
      .equalsByElementData(testGraph.toLogicalGraph()));
  }

  /**
   * Test the {@link TemporalGraph#writeTo(DataSink)} method with an unsupported sink.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedWriteTo() throws IOException {
    testGraph.writeTo(new DOTDataSink("x", true));
  }

  /**
   * Test the {@link TemporalGraph#writeTo(DataSink, boolean)} method with an unsupported sink.
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedWriteToOverwrite() throws IOException {
    testGraph.writeTo(new DOTDataSink("x", true), true);
  }

  /**
   * Test the {@link TemporalGraph#getVertices()} method.
   */
  @Test
  public void testGetVertices() throws Exception {
    List<TemporalVertex> temporalVertices = Lists.newArrayList();
    testGraph.getVertices().output(new LocalCollectionOutputFormat<>(temporalVertices));
    getExecutionEnvironment().execute();
    assertEquals(11, temporalVertices.size());
    temporalVertices.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getVerticesByLabel(String)} method.
   */
  @Test
  public void testGetVerticesByLabel() throws Exception {
    List<TemporalVertex> temporalVertices = Lists.newArrayList();
    testGraph.getVerticesByLabel("Person")
      .output(new LocalCollectionOutputFormat<>(temporalVertices));
    getExecutionEnvironment().execute();
    assertEquals(6, temporalVertices.size());
    temporalVertices.forEach(v -> assertEquals("Person", v.getLabel()));
    temporalVertices.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getEdges()} method.
   */
  @Test
  public void testGetEdges() throws Exception {
    List<TemporalEdge> temporalEdges = Lists.newArrayList();
    testGraph.getEdges().output(new LocalCollectionOutputFormat<>(temporalEdges));
    getExecutionEnvironment().execute();
    assertEquals(24, temporalEdges.size());
    temporalEdges.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getEdgesByLabel(String)} method.
   */
  @Test
  public void testGetEdgesByLabel() throws Exception {
    List<TemporalEdge> temporalEdges = Lists.newArrayList();
    testGraph.getEdgesByLabel("hasMember").output(new LocalCollectionOutputFormat<>(temporalEdges));
    getExecutionEnvironment().execute();
    assertEquals(4, temporalEdges.size());
    temporalEdges.forEach(e -> assertEquals("hasMember", e.getLabel()));
    temporalEdges.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#getGraphHead()} method.
   */
  @Test
  public void testGetGraphHead() throws Exception {
    List<TemporalGraphHead> temporalGraphHeads = Lists.newArrayList();
    testGraph.getGraphHead().output(new LocalCollectionOutputFormat<>(temporalGraphHeads));
    getExecutionEnvironment().execute();
    assertEquals(1, temporalGraphHeads.size());
    assertEquals("_DB", temporalGraphHeads.get(0).getLabel());
    temporalGraphHeads.forEach(this::checkDefaultTemporalElement);
  }

  /**
   * Test the {@link TemporalGraph#toLogicalGraph()} method.
   */
  @Test
  public void testToLogicalGraph() throws Exception {
    LogicalGraph resultingLogicalGraph = testGraph.toLogicalGraph();

    collectAndAssertTrue(resultingLogicalGraph.equalsByData(testLogicalGraph));
    collectAndAssertTrue(resultingLogicalGraph.equalsByElementData(testLogicalGraph));
    collectAndAssertTrue(resultingLogicalGraph.equalsByElementIds(testLogicalGraph));
  }
}
