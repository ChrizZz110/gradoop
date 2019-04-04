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

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;

import java.util.Objects;

/**
 * Extracts a snapshot of a temporal graph using a given temporal predicate.
 * This will calculate the subgraph of a temporal graph induced by the predicate.
 *
 * The application of this operator implies the creation of a new logical graph (i.e. graph head).
 *
 * The resulting graph will not be verified, i.e. dangling edges could occur. Use the
 * {@link TemporalGraph#verify()} operator to validate the graph.
 */
public class Snapshot implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {

  /**
   * Used temporal predicate.
   */
  private final TemporalPredicate temporalPredicate;

  /**
   * Creates an instance of the snapshot operator with the given temporal predicate.
   *
   * @param predicate The temporal predicate.
   */
  public Snapshot(TemporalPredicate predicate) {
    temporalPredicate = Objects.requireNonNull(predicate, "No predicate was given.");
  }

  @Override
  public TemporalGraph execute(TemporalGraph superGraph) {
    DataSet<TemporalVertex> vertices = superGraph.getVertices()
      // Filter vertices
      .filter(new ByTemporalPredicate<>(temporalPredicate))
      .name("Snapshot Vertices " + temporalPredicate.toString());
    DataSet<TemporalEdge> edges = superGraph.getEdges()
      // Filter edges
      .filter(new ByTemporalPredicate<>(temporalPredicate))
      .name("Snapshot Edges " + temporalPredicate.toString());

    return superGraph.getFactory().fromDataSets(vertices, edges);
  }

}
