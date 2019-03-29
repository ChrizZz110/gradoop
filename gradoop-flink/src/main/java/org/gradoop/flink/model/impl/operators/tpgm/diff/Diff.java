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
package org.gradoop.flink.model.impl.operators.tpgm.diff;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.temporal.TemporalEdge;
import org.gradoop.common.model.impl.pojo.temporal.TemporalVertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToBaseGraphOperator;
import org.gradoop.flink.model.api.tpgm.functions.TemporalPredicate;
import org.gradoop.flink.model.impl.operators.tpgm.diff.functions.DiffPerElement;
import org.gradoop.flink.model.impl.tpgm.TemporalGraph;

import java.util.Objects;

/**
 * Calculates the difference between two snapshots of a graph and stores the result in a property
 * named {@link Diff#PROPERTY_KEY}.
 * The result will be a number indicating that an element is either equal in both snapshots (0) or
 * added (1) or removed (-1) in the second snapshot. Elements not present in both snapshots will be
 * discarded.
 *
 * The application of this operator implies the creation of a new logical graph (i.e. graph head).
 *
 * The resulting graph will not be verified, i.e. dangling edges could occur. Use the
 * {@code verify()} operator to validate the graph.
 */
public class Diff implements UnaryBaseGraphToBaseGraphOperator<TemporalGraph> {
  /**
   * The property key used to store the diff result on each element.
   */
  public static final String PROPERTY_KEY = "_diff";

  /**
   * The property value used to indicate that an element was added in the second snapshot.
   */
  public static final PropertyValue VALUE_ADDED = PropertyValue.create(1);

  /**
   * The property value used to indicate that an element is equal in both snapshots.
   */
  public static final PropertyValue VALUE_EQUAL = PropertyValue.create(0);

  /**
   * The property value used to indicate that an element was removed in the second snapshot.
   */
  public static final PropertyValue VALUE_REMOVED = PropertyValue.create(-1);

  /**
   * The predicate used to determine the first snapshot.
   */
  private final TemporalPredicate firstPredicate;

  /**
   * The predicate used to determine the second snapshot.
   */
  private final TemporalPredicate secondPredicate;

  /**
   * Create an instance of the TPGM diff operator, setting the two predicates used to determine
   * the snapshots.
   *
   * @param firstPredicate  The predicate used for the first snapshot.
   * @param secondPredicate The predicate used for the second snapshot.
   */
  public Diff(TemporalPredicate firstPredicate, TemporalPredicate secondPredicate) {
    this.firstPredicate = Objects.requireNonNull(firstPredicate);
    this.secondPredicate = Objects.requireNonNull(secondPredicate);
  }

  @Override
  public TemporalGraph execute(TemporalGraph graph) {
    DataSet<TemporalVertex> transformedVertices = graph.getVertices()
      .flatMap(new DiffPerElement<>(firstPredicate, secondPredicate))
      .name("Diff Vertices of " + firstPredicate.toString() + " and " + secondPredicate.toString());
    DataSet<TemporalEdge> transformedEdges = graph.getEdges()
      .flatMap(new DiffPerElement<>(firstPredicate, secondPredicate))
      .name("Diff Edges of " + firstPredicate.toString() + " and " + secondPredicate.toString());
    return graph.getFactory().fromDataSets(transformedVertices, transformedEdges);
  }
}
