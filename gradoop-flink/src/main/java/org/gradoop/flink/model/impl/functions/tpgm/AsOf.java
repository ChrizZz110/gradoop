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
package org.gradoop.flink.model.impl.functions.tpgm;

import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;

/**
 * Implementation of the <b>AsOf</b> predicate.
 * Given a certain time-stamp, this predicate will match all time-stamps before that time
 * and all time-interval containing that time.
 */
public class AsOf implements TemporalPredicate {

  /**
   * The timestamp to be matched.
   */
  private final long timeStamp;

  /**
   * Creates a AsOf instance with the given time-stamp.
   *
   * @param timestamp The time-stamp to match.
   */
  public AsOf(long timestamp) {
    timeStamp = timestamp;
  }

  @Override
  public boolean test(Long from, Long to) {
    if (from == null && to == null) {
      // The input is always valid.
      return true;
    }
    if (to == null) {
      return from <= timeStamp;
    }
    if (from == null) {
      return timeStamp <= to;
    }
    return from <= timeStamp && timeStamp <= to;
  }
}
