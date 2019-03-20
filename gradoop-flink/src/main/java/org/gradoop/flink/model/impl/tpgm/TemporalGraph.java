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
package org.gradoop.flink.model.impl.tpgm;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.util.Preconditions;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.api.epgm.BaseGraphFactory;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayout;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseCollectionOperator;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.tpgm.TemporalGraphOperators;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.bool.Not;
import org.gradoop.flink.model.impl.functions.bool.Or;
import org.gradoop.flink.model.impl.functions.bool.True;
import org.gradoop.flink.model.impl.functions.epgm.EdgeFromTemporal;
import org.gradoop.flink.model.impl.functions.epgm.GraphHeadFromTemporal;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromTemporal;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.CypherPatternMatching;
import org.gradoop.flink.model.impl.operators.subgraph.Subgraph;
import org.gradoop.flink.model.impl.operators.transformation.Transformation;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Objects;

/**
 * A temporal (logical) graph is a base concept of the Temporal Property Graph Model (TPGM) that
 * extends the Extended Property Graph Model (EPGM). The temporal graph inherits the main concepts
 * of the {@link org.gradoop.flink.model.impl.epgm.LogicalGraph} and extends them by temporal
 * attributes. These attributes are two temporal information: the valid-time and transaction time.
 * Both are represented by a Tuple2 of Long values that specify the beginning and end time as unix
 * timestamp in milliseconds.
 *
 * transactionTime: (tx-from [ms], tx-to [ms])
 * validTime: (val-from [ms], val-to [ms])
 *
 * Furthermore, a temporal graph provides operations that are performed on the underlying data.
 * These operations result in either another temporal graph or in a {@link TemporalGraphCollection}.
 *
 * Analogous to a logical graph, a temporal graph is wrapping a layout which defines, how the graph
 * is represented in Apache Flink.
 * Note that the {@link TemporalGraph} also implements that interface and just forward the calls to
 * the layout. This is just for convenience and API synchronicity.
 */
public class TemporalGraph implements BaseGraph<TemporalGraphHead, TemporalVertex, TemporalEdge,
  TemporalGraph, TemporalGraphCollection>, TemporalGraphOperators {

  /**
   * Layout for that temporal graph.
   */
  private final LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout;
  /**
   * Configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new temporal graph instance with the given layout and gradoop flink configuration.
   *
   * @param layout the layout representing the temporal graph
   * @param config Gradoop Flink configuration
   */
  TemporalGraph(LogicalGraphLayout<TemporalGraphHead, TemporalVertex, TemporalEdge> layout,
    GradoopFlinkConfig config) {
    this.layout = Preconditions.checkNotNull(layout);
    this.config = Preconditions.checkNotNull(config);
  }

  @Override
  public GradoopFlinkConfig getConfig() {
    return this.config;
  }

  @Override
  public BaseGraphFactory<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
    TemporalGraphCollection> getFactory() {
    return this.config.getTemporalGraphFactory();
  }

  @Override
  public BaseGraphCollectionFactory<TemporalGraphHead, TemporalVertex, TemporalEdge,
    TemporalGraphCollection> getCollectionFactory() {
    return this.config.getTemporalGraphCollectionFactory();
  }

  @Override
  public DataSet<Boolean> isEmpty() {
    return getVertices().map(new True<>()).distinct()
      .union(getConfig().getExecutionEnvironment().fromElements(false)).reduce(new Or())
      .map(new Not());
  }

  @Override
  public void writeTo(DataSink dataSink) {
    throw new UnsupportedOperationException(
      "Writing a temporal graph to a DataSink is not implemented yet.");
  }

  @Override
  public void writeTo(DataSink dataSink, boolean overWrite) {
    throw new UnsupportedOperationException(
      "Writing a temporal graph to a DataSink is not implemented yet.");
  }

  @Override
  public boolean isGVELayout() {
    return this.layout.isGVELayout();
  }

  @Override
  public boolean isIndexedGVELayout() {
    return this.layout.isIndexedGVELayout();
  }

  @Override
  public DataSet<TemporalGraphHead> getGraphHead() {
    return this.layout.getGraphHead();
  }

  @Override
  public DataSet<TemporalVertex> getVertices() {
    return this.layout.getVertices();
  }

  @Override
  public DataSet<TemporalVertex> getVerticesByLabel(String label) {
    return this.layout.getVerticesByLabel(label);
  }

  @Override
  public DataSet<TemporalEdge> getEdges() {
    return this.layout.getEdges();
  }

  @Override
  public DataSet<TemporalEdge> getEdgesByLabel(String label) {
    return this.layout.getEdgesByLabel(label);
  }

  //----------------------------------------------------------------------------
  // Unary Operators
  //----------------------------------------------------------------------------

  @Override
  public TemporalGraphCollection query(String query, String constructionPattern, boolean attachData,
    MatchStrategy vertexStrategy, MatchStrategy edgeStrategy, GraphStatistics graphStatistics) {
    return callForCollection(new CypherPatternMatching<>(query, constructionPattern, attachData,
      vertexStrategy, edgeStrategy, graphStatistics));
  }

  @Override
  public TemporalGraph vertexInducedSubgraph(FilterFunction<TemporalVertex> vertexFilterFunction) {
    Objects.requireNonNull(vertexFilterFunction);
    return callForGraph(
      new Subgraph<>(vertexFilterFunction, null, Subgraph.Strategy.VERTEX_INDUCED));
  }

  @Override
  public TemporalGraph edgeInducedSubgraph(FilterFunction<TemporalEdge> edgeFilterFunction) {
    Objects.requireNonNull(edgeFilterFunction);
    return callForGraph(new Subgraph<>(null, edgeFilterFunction, Subgraph.Strategy.EDGE_INDUCED));
  }

  @Override
  public TemporalGraph subgraph(FilterFunction<TemporalVertex> vertexFilterFunction,
    FilterFunction<TemporalEdge> edgeFilterFunction, Subgraph.Strategy strategy) {
    return callForGraph(
      new Subgraph<>(vertexFilterFunction, edgeFilterFunction, strategy));
  }

  @Override
  public TemporalGraph transform(
    TransformationFunction<TemporalGraphHead> graphHeadTransformationFunction,
    TransformationFunction<TemporalVertex> vertexTransformationFunction,
    TransformationFunction<TemporalEdge> edgeTransformationFunction) {
    return callForGraph(new Transformation<>(
      graphHeadTransformationFunction,
      vertexTransformationFunction,
      edgeTransformationFunction));
  }

  //----------------------------------------------------------------------------
  // Auxiliary Operators
  //----------------------------------------------------------------------------

  @Override
  public TemporalGraph callForGraph(UnaryBaseGraphToBaseGraphOperator<TemporalGraph> operator) {
    return operator.execute(this);
  }

  @Override
  public TemporalGraphCollection callForCollection(
    UnaryBaseGraphToBaseCollectionOperator<TemporalGraph, TemporalGraphCollection> operator) {
    return operator.execute(this);
  }


  //----------------------------------------------------------------------------
  // Utilities
  //----------------------------------------------------------------------------

  @Override
  public LogicalGraph toLogicalGraph() {
    return getConfig().getLogicalGraphFactory().fromDataSets(
      getGraphHead().map(new GraphHeadFromTemporal()),
      getVertices().map(new VertexFromTemporal()),
      getEdges().map(new EdgeFromTemporal()));
  }
}
