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
package org.gradoop.flink.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster;
import org.apache.flink.test.util.TestEnvironment;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.temporal.TemporalElement;
import org.gradoop.flink.model.api.layouts.GraphCollectionLayoutFactory;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.model.impl.layouts.gve.GVECollectionLayoutFactory;
import org.gradoop.flink.model.impl.layouts.gve.GVEGraphLayoutFactory;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base class for Flink-based unit tests with the same cluster.
 */
public abstract class GradoopFlinkTestBase {

  protected static final int DEFAULT_PARALLELISM = 4;

  protected static final long TASKMANAGER_MEMORY_SIZE_MB = 512;

  protected static LocalFlinkMiniCluster CLUSTER = null;

  /**
   * Flink Execution Environment
   */
  private ExecutionEnvironment env;

  /**
   * Gradoop Flink configuration
   */
  private GradoopFlinkConfig config;

  /**
   * The factory to create a logical graph layout.
   */
  private LogicalGraphLayoutFactory<GraphHead, Vertex, Edge> graphLayoutFactory;

  /**
   * The factory to create a graph collection layout.
   */
  private GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> collectionLayoutFactory;

  /**
   * Creates a new instance of {@link GradoopFlinkTestBase}.
   */
  public GradoopFlinkTestBase() {
    TestEnvironment testEnv = new TestEnvironment(CLUSTER, DEFAULT_PARALLELISM, false);
    // makes ExecutionEnvironment.getExecutionEnvironment() return this instance
    testEnv.setAsContext();
    this.env = testEnv;
    this.graphLayoutFactory = new GVEGraphLayoutFactory();
    this.collectionLayoutFactory = new GVECollectionLayoutFactory();
  }

  /**
   * Returns the execution environment for the tests
   *
   * @return Flink execution environment
   */
  protected ExecutionEnvironment getExecutionEnvironment() {
    return env;
  }

  /**
   * Returns the configuration for the test
   *
   * @return Gradoop Flink configuration
   */
  protected GradoopFlinkConfig getConfig() {
    if (config == null) {
      setConfig(GradoopFlinkConfig.createConfig(getExecutionEnvironment(),
        graphLayoutFactory,
        collectionLayoutFactory));
    }
    return config;
  }

  /**
   * Sets the default configuration for the test
   */
  protected void setConfig(GradoopFlinkConfig config) {
    this.config = config;
  }

  protected void setCollectionLayoutFactory(
    GraphCollectionLayoutFactory<GraphHead, Vertex, Edge> collectionLayoutFactory) {
    this.collectionLayoutFactory = collectionLayoutFactory;
  }

  //----------------------------------------------------------------------------
  // Cluster related
  //----------------------------------------------------------------------------

  /**
   * Custom test cluster start routine,
   * workaround to set TASK_MANAGER_MEMORY_SIZE.
   *
   * TODO: remove, when future issue is fixed
   * {@see http://mail-archives.apache.org/mod_mbox/flink-dev/201511.mbox/%3CCAC27z=PmPMeaiNkrkoxNFzoR26BOOMaVMghkh1KLJFW4oxmUmw@mail.gmail.com%3E}
   *
   * @throws Exception
   */
  @BeforeClass
  public static void setupFlink() throws Exception {
    File logDir = File.createTempFile("TestBaseUtils-logdir", null);
    Assert.assertTrue("Unable to delete temp file", logDir.delete());
    Assert.assertTrue("Unable to create temp directory", logDir.mkdir());

    Files.createFile((new File(logDir, "jobmanager.out")).toPath());
    Path logFile =  Files
      .createFile((new File(logDir, "jobmanager.log")).toPath());

    Configuration config = new Configuration();

    config.setInteger("local.number-taskmanager", 1);
    config.setInteger("taskmanager.numberOfTaskSlots", DEFAULT_PARALLELISM);
    config.setBoolean("local.start-webserver", false);
    config.setLong("taskmanager.memory.size", TASKMANAGER_MEMORY_SIZE_MB);
    config.setBoolean("fs.overwrite-files", true);
    config.setString("akka.ask.timeout", "1000s");
    config.setString("akka.startup-timeout", "60 s");
    config.setInteger("jobmanager.web.port", 8081);
    config.setString("jobmanager.web.log.path", logFile.toString());
    CLUSTER = new LocalFlinkMiniCluster(config, true);
    CLUSTER.start();
  }

  @AfterClass
  public static void tearDownFlink() {
    CLUSTER.stop();
  }

  //----------------------------------------------------------------------------
  // Data generation
  //----------------------------------------------------------------------------

  protected FlinkAsciiGraphLoader getLoaderFromString(String asciiString) {
    FlinkAsciiGraphLoader loader = getNewLoader();
    loader.initDatabaseFromString(asciiString);
    return loader;
  }

  protected FlinkAsciiGraphLoader getLoaderFromFile(String fileName) throws IOException {
    FlinkAsciiGraphLoader loader = getNewLoader();

    loader.initDatabaseFromFile(fileName);
    return loader;
  }

  protected FlinkAsciiGraphLoader getLoaderFromStream(InputStream inputStream) throws IOException {
    FlinkAsciiGraphLoader loader = getNewLoader();

    loader.initDatabaseFromStream(inputStream);
    return loader;
  }

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  protected FlinkAsciiGraphLoader getSocialNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE);
    return getLoaderFromStream(inputStream);
  }

  protected FlinkAsciiGraphLoader getTemporalSocialNetworkLoader() throws IOException {
    InputStream inputStream = getClass()
      .getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_TEMPORAL_GDL_FILE);
    return getLoaderFromStream(inputStream);
  }

  public static Tuple2<Long, Long> extractGraphHeadTime(EPGMElement element) {
    return extractTime(element);
  }

  public static Tuple2<Long, Long> extractVertexTime(EPGMElement element) {
    return extractTime(element);
  }

  public static Tuple2<Long, Long> extractEdgeTime(EPGMElement element) {
    return extractTime(element);
  }


  public static Tuple2<Long, Long> extractTime(EPGMElement element) {
    Tuple2<Long, Long> validTime = new Tuple2<>(TemporalElement.DEFAULT_VALID_TIME_FROM,
      TemporalElement.DEFAULT_VALID_TIME_TO);

    if (element.hasProperty("__valFrom")) {
      validTime.f0 = element.getPropertyValue("__valFrom").getLong();
    }
    if (element.hasProperty("__valTo")) {
      validTime.f1 = element.getPropertyValue("__valTo").getLong();
    }
    return validTime;
  }

  /**
   * Returns an uninitialized loader with the test config.
   *
   * @return uninitialized Flink Ascii graph loader
   */
  private FlinkAsciiGraphLoader getNewLoader() {
    return new FlinkAsciiGraphLoader(getConfig());
  }

  //----------------------------------------------------------------------------
  // Test helper
  //----------------------------------------------------------------------------

  protected void collectAndAssertTrue(DataSet<Boolean> result) throws
    Exception {
    assertTrue("expected true", result.collect().get(0));
  }

  protected void collectAndAssertFalse(DataSet<Boolean> result) throws
    Exception {
    assertFalse("expected false", result.collect().get(0));
  }

  protected <T extends EPGMElement> DataSet<T> getEmptyDataSet(T dummy) {
    return getExecutionEnvironment()
      .fromElements(dummy)
      .filter(new False<>());
  }

  /**
   * Returns the encoded file path to a resource.
   *
   * @param  relPath the relative path to the resource
   * @return encoded file path
   */
  protected String getFilePath(String relPath) throws UnsupportedEncodingException {
    return URLDecoder.decode(
      getClass().getResource(relPath).getFile(), StandardCharsets.UTF_8.name());
  }

  /**
   * Check if the temporal graph element has default time values for valid and transaction time.
   *
   * @param element the temporal graph element to check
   */
  protected void checkDefaultTemporalElement(TemporalElement element) {
    assertEquals(TemporalElement.DEFAULT_VALID_TIME_FROM, element.getValidFrom());
    assertEquals(TemporalElement.DEFAULT_VALID_TIME_TO, element.getValidTo());
    checkDefaultTxTimes(element);
  }

  /**
   * Check if the temporal graph element has default time values for transaction time.
   *
   * @param element the temporal graph element to check
   */
  protected void checkDefaultTxTimes(TemporalElement element) {
    assertTrue(element.getTxFrom() < System.currentTimeMillis());
    assertEquals(TemporalElement.DEFAULT_TX_TO, element.getTxTo());
  }
}
