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
package org.gradoop.temporal.io.impl.csv.tuples;

/**
 * Interface representing a temporal CSVElement in a CSV file.
 */
public interface TemporalCSVElement {
  /**
   * Get the temporal data as String representation.
   *
   * @return a String representation of the elements temporal data
   */
  String getTemporalData();

  /**
   * Set the String representation of the temporal element.
   *
   * @param temporalData the String representation of the temporal element
   */
  void setTemporalData(String temporalData);
}
