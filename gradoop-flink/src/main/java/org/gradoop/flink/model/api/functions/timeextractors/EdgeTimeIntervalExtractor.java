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
package org.gradoop.flink.model.api.functions.timeextractors;

import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;

/**
 * Map function to extract temporal information from an EPGM edge to create a temporal edge
 * with a time interval as its validity.
 */
public interface EdgeTimeIntervalExtractor extends TimeIntervalExtractor<Edge, TemporalEdge> {

  @Override
  default TemporalEdge map(Edge edge) throws Exception {
    TemporalEdge temporalEdge = TemporalEdge.fromNonTemporalEdge(edge);
    temporalEdge.setValidFrom(getValidFrom(edge));
    temporalEdge.setValidTo(getValidTo(edge));
    return temporalEdge;
  }
}
