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
 * Aggregates the maximum value of a time value for temporal edges.
 * The value will be calculated as the maximum of a {@link TemporalAttribute.Field} of a
 * {@link TemporalAttribute}, ignoring the default value (in this case {@link Long#MAX_VALUE}).
 */
public class MaxEdgeTime extends MaxTime implements EdgeAggregateFunction {

  /**
   * Creates an instance of the {@link MaxEdgeTime} aggregate function.
   *
   * @param aggregatePropertyKey The aggregate property key.
   * @param interval             The time-interval to consider.
   * @param field                The field of the time-interval to consider.
   */
  public MaxEdgeTime(String aggregatePropertyKey, TemporalAttribute interval,
    TemporalAttribute.Field field) {
    super(aggregatePropertyKey, interval, field);
  }
}
