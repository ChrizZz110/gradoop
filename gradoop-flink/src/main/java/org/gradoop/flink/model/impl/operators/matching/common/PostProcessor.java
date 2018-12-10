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
package org.gradoop.flink.model.impl.operators.matching.common;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMEdgeFactory;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.api.entities.EPGMVertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.epgm.BaseGraph;
import org.gradoop.flink.model.api.epgm.BaseGraphCollection;
import org.gradoop.flink.model.api.epgm.BaseGraphCollectionFactory;
import org.gradoop.flink.model.impl.functions.epgm.EdgeFromIds;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.MergedGraphIds;
import org.gradoop.flink.model.impl.functions.epgm.VertexFromId;
import org.gradoop.flink.model.impl.functions.utils.Cast;
import org.gradoop.flink.model.impl.functions.utils.IsInstance;
import org.gradoop.flink.model.impl.functions.utils.RightSide;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.functions.EdgeTriple;
import org.gradoop.flink.model.impl.operators.matching.single.simulation.dual.tuples.FatVertex;

/**
 * Provides methods for post-processing query results.
 */
public class PostProcessor {

  /**
   * Extracts a {@link GC} from a set of {@link EPGMElement}.
   *
   * @param elements  EPGM elements
   * @param factory
   * @return Graph collection
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge,
    GC extends BaseGraphCollection<G, V, E, GC>> GC extractGraphCollection(DataSet<EPGMElement> elements, BaseGraphCollectionFactory<G, V, E, GC> factory) {
    return extractGraphCollection(elements, factory, true);
  }

  /**
   * Extracts a {@link GC} from a set of {@link EPGMElement}.
   *
   * @param elements  EPGM elements
   * @param factory
   * @param mayOverlap    elements may be contained in multiple graphs
   * @return Graph collection
   */
  @SuppressWarnings("unchecked")
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, GC>> GC extractGraphCollection(
    DataSet<EPGMElement> elements, BaseGraphCollectionFactory<G, V, E, GC> factory, boolean mayOverlap) {

    Class<G> graphHeadType = factory.getGraphHeadFactory().getType();
    Class<V> vertexType = factory.getVertexFactory().getType();
    Class<E> edgeType = factory.getEdgeFactory().getType();
    return factory.fromDataSets(
      extractGraphHeads(elements, graphHeadType),
      extractVertices(elements, vertexType, mayOverlap),
      extractEdges(elements, edgeType, mayOverlap)
    );
  }

  /**
   * Extracts a {@link GC} from a set of {@link EPGMElement} and
   * attaches the original data from the input {@link LG}.
   *
   * @param elements      EPGM elements
   * @param inputGraph    original input graph
   * @param mayOverlap    true, if elements may be contained in multiple graphs
   * @return Graph collection
   */
  public static <
    G extends EPGMGraphHead,
    V extends EPGMVertex,
    E extends EPGMEdge,
    LG extends BaseGraph<G, V, E, LG, GC>,
    GC extends BaseGraphCollection<G, V, E, GC>> GC extractGraphCollectionWithData(
    DataSet<EPGMElement> elements, LG inputGraph, boolean mayOverlap) {

    // get result collection without data
    GC collection = extractGraphCollection(elements, inputGraph.getCollectionFactory(), mayOverlap);

    // attach data by joining first and merging the graph head ids
    DataSet<V> newVertices = inputGraph.getVertices()
      .rightOuterJoin(collection.getVertices())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new MergedGraphIds<>())
      .withForwardedFieldsFirst("id;label;properties;");


    DataSet<E> newEdges = inputGraph.getEdges()
      .rightOuterJoin(collection.getEdges())
      .where(new Id<>()).equalTo(new Id<>())
      .with(new MergedGraphIds<>())
      .withForwardedFieldsFirst("id;label;properties");

    return collection.getFactory().fromDataSets(
      collection.getGraphHeads(), newVertices, newEdges);
  }
  /**
   * Extracts vertex ids from the given pattern matching result.
   *
   * @param result pattern matching result
   * @return dataset with (vertexId) tuples
   */
  public static DataSet<Tuple1<GradoopId>> extractVertexIds(
    DataSet<FatVertex> result) {
    return result.project(0);
  }

  /**
   * Extracts edge-source-target id triples from the given pattern matching
   * result.
   *
   * @param result pattern matching result
   * @return dataset with (edgeId, sourceId, targetId) tuples
   */
  public static DataSet<Tuple3<GradoopId, GradoopId, GradoopId>> extractEdgeIds(
    DataSet<FatVertex> result) {
    return result.flatMap(new EdgeTriple());
  }

  /**
   * Filters and casts EPGM graph heads from a given set of {@link EPGMElement}
   *
   * @param elements      EPGM elements
   * @param graphHeadType graph head type
   * @return EPGM graph heads
   */
  public static <G extends EPGMGraphHead> DataSet<G> extractGraphHeads(DataSet<EPGMElement> elements,
    Class<G> graphHeadType) {
    return elements
      .filter(new IsInstance<>(graphHeadType))
      .map(new Cast<>(graphHeadType))
      .returns(TypeExtractor.createTypeInfo(graphHeadType));
  }

  /**
   * Initializes EPGM vertices from the given pattern matching result.
   *
   * @param result        pattern matching result
   * @param epgmVertexFactory EPGM vertex factory
   * @return EPGM vertices
   */
  public static <V extends EPGMVertex> DataSet<V> extractVertices(DataSet<FatVertex> result,
                                                EPGMVertexFactory<V> epgmVertexFactory) {
    return extractVertexIds(result).map(new VertexFromId<>(epgmVertexFactory));
  }

  /**
   * Filters and casts EPGM vertices from a given set of {@link EPGMElement}
   *
   * @param elements  EPGM elements
   * @param vertexType    vertex type
   * @param mayOverlap    vertices may be contained in multiple graphs
   * @return EPGM vertices
   */
  public static <V extends EPGMVertex> DataSet<V> extractVertices(DataSet<EPGMElement> elements,
    Class<V> vertexType, boolean mayOverlap) {
    DataSet<V> result = elements
      .filter(new IsInstance<>(vertexType))
      .map(new Cast<>(vertexType))
      .returns(TypeExtractor.createTypeInfo(vertexType));
    return mayOverlap ? result
      .groupBy(new Id<>())
      .combineGroup(new MergedGraphIds<>())
      .groupBy(new Id<>())
      .reduceGroup(new MergedGraphIds<>()) : result;
  }

  /**
   * Initializes EPGM edges from the given pattern matching result.
   *
   * @param result      pattern matching result
   * @param epgmEdgeFactory EPGM edge factory
   * @return EPGM edges
   */
  public static <E extends EPGMEdge> DataSet<E> extractEdges(DataSet<FatVertex> result,
    EPGMEdgeFactory<E> epgmEdgeFactory) {
    return extractEdgeIds(result).map(new EdgeFromIds<>(epgmEdgeFactory));
  }

  /**
   * Filters and casts EPGM edges from a given set of {@link EPGMElement}
   *
   * @param elements      EPGM elements
   * @param edgeType      edge type
   * @param mayOverlap    edges may be contained in multiple graphs
   * @return EPGM edges
   */
  public static <E extends EPGMEdge> DataSet<E> extractEdges(DataSet<EPGMElement> elements,
    Class<E> edgeType, boolean mayOverlap) {
    DataSet<E> result = elements
      .filter(new IsInstance<>(edgeType))
      .map(new Cast<>(edgeType))
      .returns(TypeExtractor.createTypeInfo(edgeType));

    return mayOverlap ? result
      .groupBy(new Id<>())
      .combineGroup(new MergedGraphIds<>()).groupBy(new Id<>())
      .reduceGroup(new MergedGraphIds<>()) : result;
  }

  /**
   * Initializes EPGM vertices including their original data from the given
   * pattern matching result.
   *
   * @param result        pattern matching result
   * @param inputVertices original data graph vertices
   * @return EPGM vertices including data
   */
  public static <V extends EPGMVertex> DataSet<V> extractVerticesWithData(
    DataSet<FatVertex> result, DataSet<V> inputVertices) {
    return extractVertexIds(result)
      .join(inputVertices)
      .where(0).equalTo(new Id<>())
      .with(new RightSide<>());
  }

  /**
   * Initializes EPGM edges including their original data from the given pattern
   * matching result.
   *
   * @param result      pattern matching result
   * @param inputEdges  original data graph edges
   * @return EPGM edges including data
   */
  public static <E extends EPGMEdge> DataSet<E> extractEdgesWithData(DataSet<FatVertex> result,
    DataSet<E> inputEdges) {
    return extractEdgeIds(result)
      .join(inputEdges)
      .where(0).equalTo(new Id<>())
      .with(new RightSide<>());
  }
}
