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

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;

/**
 * Map function to extract temporal information from an EPGM graph head to create a temporal graph
 * head with a time interval as validity.
 */
public interface GraphHeadTimeIntervalExtractor
  extends TimeIntervalExtractor<GraphHead, TemporalGraphHead> {

  @Override
  default TemporalGraphHead map(GraphHead graphHead) throws Exception {
    TemporalGraphHead temporalGraphHead = TemporalGraphHead.fromNonTemporalGraphHead(graphHead);
    temporalGraphHead.setValidFrom(getValidFrom(graphHead));
    temporalGraphHead.setValidTo(getValidTo(graphHead));
    return temporalGraphHead;
  }
}
