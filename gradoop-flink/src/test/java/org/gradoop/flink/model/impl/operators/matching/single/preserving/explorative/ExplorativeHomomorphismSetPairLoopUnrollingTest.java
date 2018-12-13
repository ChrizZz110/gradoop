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
package org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.SubgraphHomomorphismTest;
import org.gradoop.flink.model.impl.operators.matching.single.preserving.explorative.traverser
  .TraverserStrategy;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;
import org.gradoop.flink.model.impl.tpgm.TemporalGraphCollection;

public class ExplorativeHomomorphismSetPairLoopUnrollingTest extends SubgraphHomomorphismTest {

  public ExplorativeHomomorphismSetPairLoopUnrollingTest(String testName,
    String dataGraph, String queryGraph, String expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Override
  public PatternMatching<GraphHead, Vertex, Edge, LogicalGraph, GraphCollection> getImplementation(String queryGraph, boolean attachData) {
    return new ExplorativePatternMatching.Builder()
      .setQuery(queryGraph)
      .setAttachData(attachData)
      .setMatchStrategy(MatchStrategy.HOMOMORPHISM)
      .setTraverserStrategy(TraverserStrategy.SET_PAIR_FOR_LOOP_ITERATION)
      .build();
  }

  @Override
  public PatternMatching<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
    TemporalGraphCollection> getTemporalImplementation(
    String queryGraph, boolean attachData) throws UnsupportedOperationException {
    throw new UnsupportedOperationException();
  }
}
