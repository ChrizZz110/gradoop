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

import java.util.Objects;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple.TempElementTuple;

/**
 * A filter function that accepts only elements matching a temporal predicate.
 * This filter operates on the simple tuple representation of temporal elements.
 *
 * @param <T> The temporal tuple type.
 */
public class ByTemporalPredicate<T extends TempElementTuple> implements FilterFunction<T> {

  /**
   * Condition to be checked.
   */
  private final TemporalPredicate condition;

  /**
   * Creates a filter instance from a temporal predicate.
   *
   * @param predicate The temporal predicate to check.
   */
  public ByTemporalPredicate(TemporalPredicate predicate) {
    condition = Objects.requireNonNull(predicate, "No predicate was given.");
  }

  @Override
  public boolean filter(T element) {
    Tuple2<Long, Long> validTime = element.getValidTime();
    return condition.test(validTime.f0, validTime.f1);
  }
}
