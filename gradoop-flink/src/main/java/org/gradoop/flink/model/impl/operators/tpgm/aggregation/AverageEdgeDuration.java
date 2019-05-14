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
package org.gradoop.flink.model.impl.operators.tpgm.aggregation;

import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;

/**
 * Calculate the average duration of a time interval of edges.
 *
 * @see AverageDuration
 */
public class AverageEdgeDuration extends AverageDuration implements EdgeAggregateFunction {

  /**
   * Create an instance of the {@link AverageEdgeDuration} aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param interval             The time-interval to consider.
   */
  public AverageEdgeDuration(String aggregatePropertyKey, TemporalAttribute interval) {
    super(aggregatePropertyKey, interval);
  }
}
