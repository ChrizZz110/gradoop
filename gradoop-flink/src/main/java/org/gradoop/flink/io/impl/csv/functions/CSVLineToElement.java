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
package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.metadata.MetaData;
import org.gradoop.common.model.impl.metadata.PropertyMetaData;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.csv.CSVConstants;
import org.gradoop.flink.io.impl.csv.CSVDataSource;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaData;
import org.gradoop.flink.io.impl.csv.metadata.CSVMetaDataSource;

import java.io.IOException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Base class for reading an {@link EPGMElement} from CSV. Handles the {@link MetaData} which is
 * required to parse the property values.
 *
 * @param <E> EPGM element type
 */
abstract class CSVLineToElement<E extends EPGMElement> extends RichMapFunction<String, E> {
  /**
   * Temporal data pattern to parse the temporal string
   */
  static final Pattern TEMPORAL_PATTERN =
    Pattern.compile("\\((-?\\d+),(-?\\d+)\\),\\((-?\\d+),(-?\\d+)\\)");
  /**
   * Stores the properties for the {@link EPGMElement} to be parsed.
   */
  private final Properties properties;
  /**
   * Meta data that provides parsers for a specific {@link EPGMElement}.
   */
  private CSVMetaData metaData;

  /**
   * Constructor
   */
  CSVLineToElement() {
    this.properties = Properties.create();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.metaData = new CSVMetaDataSource().fromTuples(getRuntimeContext()
      .getBroadcastVariable(CSVDataSource.BC_METADATA));
  }

  /**
   * Parses the given property values according to the meta data associated with the specified
   * label.
   *
   * @param type                element type
   * @param label               element label
   * @param propertyValueString string representation of elements' property values
   * @return parsed properties
   */
  Properties parseProperties(String type, String label, String propertyValueString) {
    String[] propertyValues = StringEscaper
      .split(propertyValueString, CSVConstants.VALUE_DELIMITER);
    List<PropertyMetaData> metaDataList = metaData.getPropertyMetaData(type, label);
    properties.clear();
    for (int i = 0; i < propertyValues.length; i++) {
      if (propertyValues[i].length() > 0) {
        properties.set(metaDataList.get(i).getKey(),
          metaDataList.get(i).getValueParser().apply(propertyValues[i]));
      }
    }
    return properties;
  }

  /**
   * Parses the CSV string that contains GraphHead ids.
   *
   * @param gradoopIdsString The csv token string.
   * @return gradoop ids contained in the string
   */
  GradoopIdSet parseGradoopIds(String gradoopIdsString) {
    String[] gradoopIds = gradoopIdsString
      .substring(1, gradoopIdsString.length() - 1)
      .split(CSVConstants.LIST_DELIMITER);

    GradoopIdSet gradoopIdSet = new GradoopIdSet();
    for (String g : gradoopIds) {
      gradoopIdSet.add(GradoopId.fromString(g.trim()));
    }
    return gradoopIdSet;
  }

  /**
   * Validates the temporal data matched by the given matcher instance.
   *
   * @param matcher the matcher instance containing the temporal data
   * @throws IOException if the temporal attributes can not be found inside the string
   */
  void validateTemporalData(Matcher matcher) throws IOException {
    if (!matcher.matches() || matcher.groupCount() != 4) {
      throw new IOException("Can not read temporal data from csv line of edge file.");
    }
  }

  /**
   * Parses the transaction time from the temporal data matched by the given matcher instance.
   *
   * @param matcher the matcher instance containing the temporal data
   * @return a tuple containing the transaction time
   */
  Tuple2<Long, Long> parseTransactionTime(Matcher matcher) {
    return new Tuple2<>(Long.valueOf(matcher.group(1)), Long.valueOf(matcher.group(2)));
  }

  /**
   * Parses the valid time from the temporal data matched by the given matcher instance.
   *
   * @param matcher the matcher instance containing the temporal data
   * @return a tuple containing the valid time
   */
  Tuple2<Long, Long> parseValidTime(Matcher matcher) {
    return new Tuple2<>(Long.valueOf(matcher.group(3)), Long.valueOf(matcher.group(4)));
  }

  /**
   * Splits the specified string.
   *
   * @param s     string
   * @param limit resulting array length
   * @return tokens
   */
  public String[] split(String s, int limit) {
    return StringEscaper.split(s, CSVConstants.TOKEN_DELIMITER, limit);
  }
}
