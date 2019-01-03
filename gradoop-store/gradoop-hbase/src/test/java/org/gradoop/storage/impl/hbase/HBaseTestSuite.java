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
package org.gradoop.storage.impl.hbase;

import org.gradoop.storage.impl.hbase.io.HBaseDataSinkSourceTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test Suite class to make sure HBase and Flink test instances are started only once for all tests.
 *
 * Usage: append TestClasses to SuiteClasses to run them within the suite.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
  HBaseEPGMStoreTest.class,
  HBaseDefaultGraphStoreTest.class,
  HBaseSplitRegionGraphStoreTest.class,
  HBaseSpreadingByteGraphStoreTest.class,
  HBaseDataSinkSourceTest.class
})
public class HBaseTestSuite {

  @BeforeClass
  public static void setupHBase() throws Exception {
    GradoopHBaseTestBase.setUpHBase();
  }

  @AfterClass
  public static void tearDownHBase() throws Exception {
    GradoopHBaseTestBase.tearDownHBase();
  }
}
