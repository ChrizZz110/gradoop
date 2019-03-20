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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.api.entities.EPGMGraphHeadFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.temporal.TemporalGraphHead;
import org.gradoop.flink.model.api.functions.TimeIntervalExtractor;

/**
 * Initializes a {@link TemporalGraphHead} from a {@link GraphHead} instance by setting either
 * default temporal information or, if a timeIntervalExtractor is given, by the extracted time
 * information.
 */
@FunctionAnnotation.ForwardedFields("id;label;properties")
public class TemporalGraphHeadFromNonTemporal implements MapFunction<GraphHead, TemporalGraphHead> {
  /**
   * The user defined interval extractor, might be {@code null}.
   */
  private TimeIntervalExtractor<GraphHead> timeIntervalExtractor;
  /**
   * Reuse this instance to reduce instantiations.
   */
  private TemporalGraphHead reuse;

  /**
   * Creates an instance of the TemporalGraphHeadFromNonTemporal map function. The temporal instance
   * will have default temporal information.
   *
   * @param elementFactory factory that is responsible for creating a temporal graph head instance
   */
  public TemporalGraphHeadFromNonTemporal(EPGMGraphHeadFactory<TemporalGraphHead> elementFactory) {
    this.reuse = elementFactory.createGraphHead();
  }

  /**
   * Creates an instance of the TemporalGraphHeadFromNonTemporal map function. The temporal instance
   * will have valid times extracted from the non-temporal instance by the given
   * timeIntervalExtractor.
   *
   * @param elementFactory factory that is responsible for creating a temporal graph head instance
   * @param timeIntervalExtractor the extractor instance fetches the validFrom and validTo values
   */
  public TemporalGraphHeadFromNonTemporal(
    EPGMGraphHeadFactory<TemporalGraphHead> elementFactory,
    TimeIntervalExtractor<GraphHead> timeIntervalExtractor) {
    this(elementFactory);
    this.timeIntervalExtractor = timeIntervalExtractor;
  }

  /**
   * Creates a temporal graph head instance from the non-temporal. Id's, label and properties will
   * be kept. If a timeIntervalExtractor is given, the valid time interval will be set with the
   * extracted information.
   *
   * @param value the non-temporal element
   * @return the temporal element
   * @throws Exception on failure
   */
  @Override
  public TemporalGraphHead map(GraphHead value) throws Exception {
    reuse.setId(value.getId());
    reuse.setLabel(value.getLabel());
    reuse.setProperties(value.getProperties());
    if (timeIntervalExtractor != null) {
      reuse.setValidTime(timeIntervalExtractor.map(value));
    }
    return reuse;
  }
}
