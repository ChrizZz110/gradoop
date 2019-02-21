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
package org.gradoop.flink.model.impl.operators.tpgm.snapshot.tuple;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * Defines the functional interface of a tuple representation of temporal graph elements.
 */
public interface TempElementTuple {
  /**
   * Get the valid time tuple (valid-from, valid-to).
   *
   * @return a tuple 2 representing the valid time interval
   */
  Tuple2<Long, Long> getValidTime();
}
