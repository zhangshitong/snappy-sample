package snappydata

import java.util.Properties

import com.pivotal.gemfirexd.FabricService
import io.snappydata.cluster.ClusterManagerTestBase.{locatorPort, startSnappyLead}
import io.snappydata.{Lead, Property, ServiceManager}
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.{SaveMode, SnappyContext}

import scala.util.Random

/**
  * Created by STZHANG on 2017/11/14.
  */
object ColumnTableTest {
  def main(arg: Array[String]): Unit={

    val tName1 = ColumnFormatRelation.columnBatchTableName("PARTSUPP".toUpperCase())
    println(tName1+"")
    //init properties;
    val bootProps: Properties = new Properties()
    val sysProps: Properties = new Properties()
    initProperties(bootProps, sysProps);
    val locatorPort = 10334;
    //start as lead
    val sc = SnappyContext.globalSparkContext
    if (sc == null || sc.isStopped) {
      println("start snappylead")
      startSnappyLead(locatorPort, "192.168.56.2", bootProps.clone().asInstanceOf[java.util.Properties])
    }
    assert(ServiceManager.currentFabricServiceInstance.status ==
      FabricService.State.RUNNING);

    //test code.
    val snc = SnappyContext(sc);

    val tableName = "NEW_CUSTOMER";
    val tName = ColumnFormatRelation.columnBatchTableName(tableName.toUpperCase())
    println("prepared 1008 data....")
    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0), Seq(3, 9, 3))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, 3).map(s => new Data(s(0), s(1), s(2)))


    val dataDF = snc.createDataFrame(rdd)
    val p = Map.empty[String, String]
    //create a column table.
    println("prepared to create column table.")
    snc.createTable(tableName, "column", dataDF.schema, p)
    dataDF.write.mode(SaveMode.Append).saveAsTable(tableName)

    //Get result.
    println("check the result via sql.")
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect();
    assert(r.length == 1008, s"Unexpected elements ${r.length}, expected=1008")
    //check from region




  }




  /**
    * Start a snappy lead. This code starts a Spark server and at the same time
    * also starts a SparkContext and hence it kind of becomes lead. We will use
    * LeadImpl once the code for that is ready.
    *
    * Only a single instance of SnappyLead should be started.
    */
  def startSnappyLead(locatorPort: Int, locatorAddress: String, props: Properties): Unit = {
    props.setProperty("locators", locatorAddress+"[" + locatorPort + ']')
    props.setProperty(Property.JobServerEnabled.name, "false")
    props.setProperty("isTest", "true")
    val server: Lead = ServiceManager.getLeadInstance
    server.start(props)
    assert(server.status == FabricService.State.RUNNING)
  }



  def initProperties(bootProps: Properties, sysProps: Properties): Unit ={
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
    bootProps.setProperty("spark.memory.manager",
      "org.apache.spark.memory.SnappyUnifiedMemoryManager")
    bootProps.setProperty("critical-heap-percentage", "95")

    // reduce startup time
    sysProps.setProperty("p2p.discoveryTimeout", "1000")
    sysProps.setProperty("p2p.joinTimeout", "2000")
    sysProps.setProperty("p2p.minJoinTries", "1")
  }
}
