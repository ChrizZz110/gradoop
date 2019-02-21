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
package org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;

/**
 * Creates a simpler tuple-based representation of a temporal vertex.
 */
public class TemporalVertexToTempVertexTuple
  implements MapFunction<TemporalVertex, TempVertexTuple> {

  /**
   * Reduce object instantiations.
   */
  private final TempVertexTuple reuseTuple = new TempVertexTuple();

  @Override
  public TempVertexTuple map(TemporalVertex value) {
    reuseTuple.f0 = value.getId();
    reuseTuple.f1 = value.getValidTime();
    return reuseTuple;
  }
}
