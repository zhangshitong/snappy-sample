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

import java.net.InetAddress

import scala.math._
import scala.util.Random

import org.apache.spark.sql.{Row, SnappyContext}
import org.apache.spark.{SparkConf, SparkContext}

class ClusterMgrDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  import ClusterMgrDUnitTest._

  /**
   * This test starts a lead node and two server nodes. Executes a job.
   * Then stops the lead node and starts lead in another node and then executes
   * the same job.
   */
  def testMultipleDriver(): Unit = {
    // Execute the job
    startSparkJob()
    startGemJob()
    // Stop the lead node
    ClusterManagerTestBase.stopSpark()

    // Start the lead node in another JVM. The executors should
    // connect with this new lead.
    // In this case servers are already running and a lead comes
    // and join
    try {
      vm3.invoke(getClass, "stopAny")
      vm3.invoke(getClass, "startSnappyLead", startArgs)
      vm3.invoke(getClass, "startSparkJob")
      vm3.invoke(getClass, "startGemJob")
    } finally {
      vm3.invoke(getClass, "stopSpark")
      ClusterManagerTestBase.startSnappyLead(ClusterManagerTestBase.locatorPort, bootProps)
    }
  }

  def testUncaughtExceptionInExecutor(): Unit = {
    try {
      failTheExecutors
    } catch {
      case _ : Throwable =>
    }
    // The executors should have started automatically, so this should not hang
    startSparkJob()
  }

  def testUncaughtExceptionInExecutorthread(): Unit = {
    vm2.invoke(getClass, "failAThread")
    vm1.invoke(getClass, "failAThread")
    vm0.invoke(getClass, "failAThread")
    // The executors should have started automatically, so this should not hang
    startSparkJob()
  }

  def testSnap684(): Unit = {
    startSparkJob()
    startGemJob()
    vm3.invoke(getClass, "stopAny")
    vm3.invoke(getClass, "startExternalSparkApp", ClusterManagerTestBase.locatorPort)
  }
}

object ClusterMgrDUnitTest {

  private def sc = SnappyContext.globalSparkContext

  def startSparkJob(): Unit = {
    val slices = 5
    val n = math.min(1000000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = sc.parallelize(1 until n, slices).map { i =>
        val x = random * 2 - 1
        val y = random * 2 - 1
        if (x * x + y * y < 1) 1 else 0
      }.reduce(_ + _)
    val pi = 4.0 * count / n
    assert(3.04 <= pi)
    assert(3.25 > pi)
  }

  def failTheExecutors: Unit = {
    sc.parallelize(1 until 100, 5).map { i =>
      throw new OutOfMemoryError("Some message")
    }.collect()
  }

  def failAThread: Unit = {
    new Thread(){
      override def run(): Unit = {
        throw new InternalError();
      }
    }.start()
  }

  def startGemJob(): Unit = {

    val snContext = SnappyContext(sc)
    val externalUrl = "jdbc:snappydata:;"
    val ddlStr = "YearI INT NOT NULL," +
        "MonthI INT NOT NULL," +
        "DayOfMonth INT NOT NULL," +
        "DepTime INT," +
        "ArrTime INT," +
        "UniqueCarrier CHAR(6) NOT NULL"
    snContext.sql("drop table if exists airline")
    snContext.sql(s"create table airline ($ddlStr)")
    if (new Random().nextBoolean()) {

      snContext.sql(s"create external table airline1 " +
          s" using jdbc options (URL '$externalUrl'," +
          "  Driver 'io.snappydata.jdbc.EmbeddedDriver', dbtable 'APP.AIRLINE')").collect()
    } else {
      snContext.sql(s"create external table if not exists airline1 " +
          s" using jdbc options (URL '$externalUrl'," +
          "  Driver 'com.pivotal.gemfirexd.jdbc.EmbeddedDriver',dbtable 'APP.AIRLINE')").collect()
    }

    snContext.sql("insert into airline values(2015, 2, 15, 1002, 1803, 'AA')")
    snContext.sql("insert into airline values(2014, 4, 15, 1324, 1500, 'UT')")

    val result = snContext.sql("select * from airline1")
    val expected = Set[Row](Row(2015, 2, 15, 1002, 1803, "AA    "),
        Row(2014, 4, 15, 1324, 1500, "UT    "))
    val returnedRows = result.collect()
    // scalastyle:off
    println(s"Returned rows: ${returnedRows.mkString(",")} ")
    println(s"Expected rows: ${expected.mkString(",")}")
    // scalastyle:on
    assert(returnedRows.toSet == expected)

    snContext.sql("drop table if exists airline")
    snContext.sql("drop table if exists airline1")
  }

  def startExternalSparkApp(locatorPort: Int): Unit = {
    //    println("locatorPort =" + locatorPort)
    val hostName = InetAddress.getLocalHost.getHostName
    val conf: SparkConf = new SparkConf()
        .setMaster(s"snappydata://$hostName:$locatorPort")
        .setAppName("externalApp").set("spark.testing.reservedMemory", "0")

    try {
      new SparkContext(conf)
      assert(assertion = false,
        "Expected SparkContext creation to fail due to existing lead")
    } catch {
      case e: org.apache.spark.SparkException =>
        if (!e.getMessage.startsWith("Primary Lead node (Spark Driver) is " +
            "already running in the system")) {
          throw e
        } // else ok
    }
  }
}
