/*
 * Copyright (c) 2017 SnappyData, Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.cluster

import java.sql.{Connection, DriverManager}
import java.util.Properties

import scala.language.postfixOps
import scala.sys.process._

import com.pivotal.gemfirexd.internal.engine.Misc
import com.pivotal.gemfirexd.internal.engine.distributed.utils.GemFireXDUtils
import com.pivotal.gemfirexd.{FabricService}
import io.snappydata._
import io.snappydata.test.dunit.DistributedTestBase.WaitCriterion
import io.snappydata.test.dunit._
import org.slf4j.LoggerFactory

import org.apache.spark.sql.SnappyContext
import org.apache.spark.sql.collection.Utils
import org.apache.spark.sql.execution.ConnectionPool
import org.apache.spark.{Logging, SparkContext}
/**
 * Base class for tests using Snappy ClusterManager. New utility methods
 * would need to be added as and when corresponding snappy code gets added.
 *
 * @author hemant
 */
abstract class ClusterManagerTestBase(s: String)
    extends DistributedTestBase(s) with Serializable {

  import ClusterManagerTestBase._

  val bootProps: Properties = new Properties()
  val sysProps: Properties = new Properties()
  bootProps.setProperty("log-file", "snappyStore.log")
  val logLevel: String = System.getProperty("logLevel", "config")
  bootProps.setProperty("log-level", logLevel)
  // set DistributionManager.VERBOSE for log-level fine or higher
  if (logLevel.startsWith("fine") || logLevel == "all") {
    sysProps.setProperty("DistributionManager.VERBOSE", "true")
  }
  bootProps.setProperty("security-log-level",
    System.getProperty("securityLogLevel", "config"))
  // Easier to switch ON traces. thats why added this.
//   bootProps.setProperty("gemfirexd.debug.true",
//     "QueryDistribution,TraceExecution,TraceActivation,TraceTran")
  bootProps.setProperty("statistic-archive-file", "snappyStore.gfs")
  bootProps.setProperty("spark.executor.cores","2");
//  bootProps.setProperty("spark.driver.memory","2g");
  bootProps.setProperty("spark.memory.manager",
    "org.apache.spark.memory.SnappyUnifiedMemoryManager")
//  bootProps.setProperty("critical-heap-percentage", "75")

  // reduce startup time
  sysProps.setProperty("p2p.discoveryTimeout", "1000")
  sysProps.setProperty("p2p.joinTimeout", "2000")
  sysProps.setProperty("p2p.minJoinTries", "1")

  var host: Host = _
  var vm0: VM = _
  var vm1: VM = _
  var vm2: VM = _
  var vm3: VM = _

  if (Host.getHostCount > 0) {
    host = Host.getHost(0)
    vm0 = host.getVM(0)
    vm1 = host.getVM(1)
    vm2 = host.getVM(2)
    vm3 = host.getVM(3)
  }

  protected final def startArgs =
    Array(locatorPort, bootProps).asInstanceOf[Array[AnyRef]]

  val locatorNetPort: Int = 1527
  val locatorNetProps = new Properties()
  val stopNetServersInTearDown = true

  // SparkContext is initialized on the lead node and hence,
  // this can be used only by jobs running on Lead node
  def sc: SparkContext = SnappyContext.globalSparkContext

  override def beforeClass(): Unit = {
    super.beforeClass()
    doSetUp()
    val locNetPort = locatorNetPort
    val locNetProps = locatorNetProps
    locNetProps.setProperty("hostname-for-clients","192.168.56.1");

    val locPort = ClusterManagerTestBase.locPort
    val sysProps = this.sysProps
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        ClusterManagerTestBase.setSystemProperties(sysProps)
        val loc: Locator = ServiceManager.getLocatorInstance
        val logger = LoggerFactory.getLogger(getClass)
        println("locator status: "+loc.status)
        if (loc.status != FabricService.State.RUNNING) {
          println("locator is starting on port:"+locPort)
          loc.start("192.168.56.1", locPort, locNetProps)
          println("locator is start success.")
        }
        if (locNetPort > 0) {
          loc.startNetworkServer("192.168.56.1", locNetPort, locNetProps)
        }
        assert(loc.status == FabricService.State.RUNNING)

        logger.info("\n\n\n  STARTING TESTS IN " + getClass.getName + "\n\n")
      }
    })
    val nodeProps = bootProps
    val startNode = new SerializableRunnable() {
      override def run(): Unit = {
        ClusterManagerTestBase.setSystemProperties(sysProps)
        val node = ServiceManager.currentFabricServiceInstance
        if (node == null || node.status != FabricService.State.RUNNING) {
          startSnappyServer(locPort, nodeProps)
        }
        assert(ServiceManager.currentFabricServiceInstance.status ==
            FabricService.State.RUNNING)

        val logger = LoggerFactory.getLogger(getClass)
        logger.info("\n\n\n  STARTING TESTS IN " + getClass.getName + "\n\n")
      }
    }

    vm0.invoke(startNode)
    Array(vm1, vm2).map(_.invokeAsync(startNode)).foreach(_.getResult)
    vm3.invoke(new SerializableRunnable() {
      override def run(): Unit = {
        ClusterManagerTestBase.setSystemProperties(sysProps)
      }
    })
    // start lead node in this VM
    val sc = SnappyContext.globalSparkContext
    if (sc == null || sc.isStopped) {
      startSnappyLead(locatorPort, bootProps.clone().asInstanceOf[java.util.Properties])
    }
    assert(ServiceManager.currentFabricServiceInstance.status ==
        FabricService.State.RUNNING)
  }

  override def setUp(): Unit = {
    super.setUp()
    doSetUp()
  }

  private def doSetUp() : Unit = {
    val testName = getName
    val testClass = getClass
    // bootProps.setProperty(Attribute.SYS_PERSISTENT_DIR, s)
    ClusterManagerTestBase.setSystemProperties(sysProps)
    GemFireXDUtils.IS_TEST_MODE = true

    getLogWriter.info("\n\n\n  STARTING TEST " + testClass.getName + '.' +
        testName + "\n\n")
  }

  override def tearDown2(): Unit = {
    super.tearDown2()
    GemFireXDUtils.IS_TEST_MODE = false
    cleanupTestData(getClass.getName, getName)
    Array(vm3, vm2, vm1, vm0).foreach(_.invoke(getClass, "cleanupTestData",
      Array[AnyRef](getClass.getName, getName)))
    if (stopNetServersInTearDown) {
      Array(vm3, vm2, vm1, vm0).foreach(_.invoke(getClass, "stopNetworkServers"))
      stopNetworkServers()
    }
    
    bootProps.clear()
  }

  override def afterClass(): Unit = {
    super.afterClass()
    val locNetPort = locatorNetPort
    DistributedTestBase.invokeInLocator(new SerializableRunnable() {
      override def run(): Unit = {
        if (locNetPort > 0) {
          val loc = ServiceManager.getLocatorInstance
          if (loc != null) {
            loc.stopAllNetworkServers()
          }
        }
      }
    })
  }

  def getANetConnection(netPort: Int,
      useGemXDURL: Boolean = false,
      disableQueryRouting: Boolean = false): Connection = {
    val driver = "io.snappydata.jdbc.ClientDriver"
    Utils.classForName(driver).newInstance
    var url: String = null
    if (useGemXDURL) {
      url = "jdbc:gemfirexd:thrift://localhost:" + netPort + "/"
    } else if (disableQueryRouting) {
      url = "jdbc:snappydata://localhost:" + netPort + "/route-query=false"
    } else {
      url = "jdbc:snappydata://localhost:" + netPort + "/"
    }

    DriverManager.getConnection(url)
  }

  def startNetworkServersOnAllVMs(): Unit = {
    vm0.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm1.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
    vm2.invoke(classOf[ClusterManagerTestBase], "startNetServer",
      AvailablePortHelper.getRandomAvailableTCPPort)
  }



}

/**
 * New utility methods would need to be added as and when corresponding
 * snappy code gets added.
 */
object ClusterManagerTestBase extends Logging {
  final def locatorPort: Int = DistributedTestBase.getDUnitLocatorPort
  final lazy val locPort: Int = locatorPort

  /* SparkContext is initialized on the lead node and hence,
  this can be used only by jobs running on Lead node */
  def sc: SparkContext = SnappyContext.globalSparkContext

  def setSystemProperties(props: Properties): Unit = {
    val sysPropNames = props.stringPropertyNames().iterator()
    while (sysPropNames.hasNext) {
      val propName = sysPropNames.next()
      System.setProperty(propName, props.getProperty(propName))
    }
  }

  /**
   * Start a snappy lead. This code starts a Spark server and at the same time
   * also starts a SparkContext and hence it kind of becomes lead. We will use
   * LeadImpl once the code for that is ready.
   *
   * Only a single instance of SnappyLead should be started.
   */
  def startSnappyLead(locatorPort: Int, props: Properties): Unit = {
    props.setProperty("locators", "192.168.56.1[" + locatorPort + ']')
    props.setProperty(Property.JobServerEnabled.name, "false")
    props.setProperty("isTest", "true")
    val server: Lead = ServiceManager.getLeadInstance
    server.start(props)
    assert(server.status == FabricService.State.RUNNING)
  }

  /**
   * Start a snappy server. Any number of snappy servers can be started.
   */
  def startSnappyServer(locatorPort: Int, props: Properties): Unit = {
    println("start snapppy server locatorPort: "+locatorPort);
    props.setProperty("locators", "192.168.56.1[" + locatorPort + ']')
    // bootProps.setProperty("log-level", "info")
    val server: Server = ServiceManager.getServerInstance

    server.start(props)
    assert(server.status == FabricService.State.RUNNING)
  }

  def startNetServer(netPort: Int): Unit = {
    ServiceManager.getServerInstance.startNetworkServer("192.168.56.1",
      netPort, null)
  }

  def cleanupTestData(testClass: String, testName: String): Unit = {
    // cleanup metastore
    val snc = SnappyContext()
    if (snc != null) {
    }
    if (testName != null) {
      logInfo("\n\n\n  ENDING TEST " + testClass + '.' + testName + "\n\n")
    }
  }

  def stopSpark(): Unit = {
    // cleanup metastore
    cleanupTestData(null, null)
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      service.stop(null)
    }
  }

  def stopNetworkServers(): Unit = {
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      service.stopAllNetworkServers()
      // clear stale connection pool
      ConnectionPool.clear()
    }
  }

  def stopAny(): Unit = {
    val service = ServiceManager.currentFabricServiceInstance
    if (service != null) {
      service.stop(null)
    }
  }

  /**
    * Wait until given criterion is met
    *
    * @param check          Function criterion to wait on
    * @param ms             total time to wait, in milliseconds
    * @param interval       pause interval between waits
    * @param throwOnTimeout if false, don't generate an error
    */
  def waitForCriterion(check: => Boolean, desc: String, ms: Long,
      interval: Long, throwOnTimeout: Boolean): Unit = {
    val criterion = new WaitCriterion {

      override def done: Boolean = {
        check
      }

      override def description(): String = desc
    }
    DistributedTestBase.waitForCriterion(criterion, ms, interval,
      throwOnTimeout)
  }

  def startSparkCluster(productDir: String): Unit = {
    logInfo(s"Starting spark cluster in $productDir/work")
    (productDir + "/sbin/start-all.sh") !!
  }

  def stopSparkCluster(productDir: String): Unit = {
    val sparkContext = SnappyContext.globalSparkContext
    logInfo(s"Stopping spark cluster in $productDir/work")
    if (sparkContext != null) sparkContext.stop()
    (productDir + "/sbin/stop-all.sh") !!
  }

  def validateNoActiveSnapshotTX(): Unit = {
    val cache = Misc.getGemFireCache
    val txMgr = cache.getCacheTransactionManager
    if (txMgr != null) {
      val itr = txMgr.getHostedTransactionsInProgress.iterator()
      while (itr.hasNext) {
        val tx = itr.next()
        if (tx.isSnapshot) assert(tx.isClosed, s"$tx is not closed. ")
      }
    }
  }
}
