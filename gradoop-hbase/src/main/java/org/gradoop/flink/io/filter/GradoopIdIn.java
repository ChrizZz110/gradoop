/**
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
package org.gradoop.flink.io.filter;

import org.apache.hadoop.hbase.filter.*;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Expression class for predicate push-down to define a set of edge ids.
 * An object of this class can be added to a list which can be applied to
 * a FilterableDataSource instance.
 */
public class GradoopIdIn implements EdgeFilter, VertexFilter {

  /**
   * A set of edge ids to filter
   */
  private GradoopIdSet gradoopIds;

  /**
   * Creates a new IN-Expression for edge-ids
   *
   * @param gradoopIds a GradoopIdSet of edge-ids to filter
   */
  public GradoopIdIn(GradoopIdSet gradoopIds) {
    this.gradoopIds = gradoopIds;

  }

  @Override
  public Filter getHBaseFilter() {
    FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
    RowFilter rowFilter;
    for (GradoopId gradoopId : this.gradoopIds) {
      rowFilter = new RowFilter(
          CompareFilter.CompareOp.EQUAL,
          new BinaryComparator(gradoopId.toByteArray())
      );
      filterList.addFilter(rowFilter);
    }
    return filterList;
  }
}
