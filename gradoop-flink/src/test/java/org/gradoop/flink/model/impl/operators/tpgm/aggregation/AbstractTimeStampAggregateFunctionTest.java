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
package org.gradoop.flink.model.impl.operators.tpgm.aggregation;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.tpgm.functions.TemporalAttribute;
import org.gradoop.flink.model.api.tpgm.functions.aggregation.TemporalAggregateFunction;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

/**
 * Test for the {@link AbstractTimeStampAggregateFunction}. This test will check if
 * the correct temporal attribute is read from an element.
 */
@RunWith(Parameterized.class)
public class AbstractTimeStampAggregateFunctionTest extends GradoopFlinkTestBase {

  /**
   * The element used as an input to the aggregate function in this test.
   */
  private TemporalElement testElement;

  /**
   * The interval to consider.
   */
  @Parameterized.Parameter
  public TemporalAttribute interval;

  /**
   * The field in the interval.
   */
  @Parameterized.Parameter(1)
  public TemporalAttribute.Field field;

  /**
   * The expected value.
   */
  @Parameterized.Parameter(2)
  public long expectedValue;

  /**
   * Set up this test.
   */
  @Before
  public void setUp() {
    testElement = getConfig().getTemporalVertexFactory().createVertex();
    testElement.setTransactionTime(Tuple2.of(1L, 2L));
    testElement.setValidTime(Tuple2.of(3L, 4L));
  }

  /**
   * Test if the {@link AbstractTimeStampAggregateFunction#getIncrement(TemporalElement)} returns
   * the correct field.
   */
  @Test
  public void testGetIncrement() {
    // Create a mock of the abstract function. (The first constructor parameter, the property key,
    // is irrelevant for this test.
    TemporalAggregateFunction mock = mock(AbstractTimeStampAggregateFunction.class, withSettings()
      .useConstructor("", interval, field).defaultAnswer(CALLS_REAL_METHODS));
    PropertyValue increment = mock.getIncrement(testElement);
    assertTrue(increment.isLong());
    assertEquals(expectedValue, increment.getLong());
  }

  /**
   * Parameters for this test.
   * Those are:
   * <ol>
   * <li>The time interval used.</li>
   * <li>The field of the time interval used.</li>
   * <li>The expected value.</li>
   * </ol>
   *
   * @return Parameters in the given format.
   */
  @Parameterized.Parameters(name = "{1} of {0}")
  public static Object[][] parameters() {
    return new Object[][] {
      {TemporalAttribute.TRANSACTION_TIME, TemporalAttribute.Field.FROM, 1L},
      {TemporalAttribute.TRANSACTION_TIME, TemporalAttribute.Field.TO, 2L},
      {TemporalAttribute.VALID_TIME, TemporalAttribute.Field.FROM, 3L},
      {TemporalAttribute.VALID_TIME, TemporalAttribute.Field.TO, 4L}
    };
  }
}
