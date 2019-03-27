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
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;
import org.gradoop.flink.model.impl.functions.epgm.TargetId;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;

import java.util.Objects;

/**
 * Extracts a snapshot of a temporal graph using a given temporal predicate.
 * This will calculate the subgraph of a temporal graph induced by the predicate.
 * The resulting graph will be verified, i.e. dangling edges will be removed.
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
      .name("Snapshot Edges " + temporalPredicate.toString())
      // Validate edges
      .join(vertices).where(new SourceId<>()).equalTo(new Id<>()).with(new LeftSide<>())
      .name("Verify Edges (1/2)")
      .join(vertices).where(new TargetId<>()).equalTo(new Id<>()).with(new LeftSide<>())
      .name("Verify Edges (2/2)");

    return superGraph.getFactory().fromDataSets(superGraph.getGraphHead(), vertices, edges);
  }

}
