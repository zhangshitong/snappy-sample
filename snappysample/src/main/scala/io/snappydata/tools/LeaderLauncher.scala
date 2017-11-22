package io.snappydata.tools

import java.net.InetAddress
import java.util.Properties

import com.pivotal.gemfirexd.FabricService
import io.snappydata.tools.LocLauncher.{locatorNetProps, memberName, workHome}
import io.snappydata.tools.NodeLauncher.{memberName, nodeProps, sysProps, workHome}
import io.snappydata.{Lead, Property, Server, ServiceManager}
import org.slf4j.LoggerFactory

import scala.reflect.io.{File, Path}
import scala.util.control.Breaks

/**
  * Created by STZHANG on 2017/11/20.
  */
object LeaderLauncher {

  val workHome = "D:/apps/snappydata/work";
  val memberName = "lead1";

  val logger = LoggerFactory.getLogger(getClass)
//  val clientAddress = InetAddress.getLocalHost.getHostAddress;
  val sysProps: Properties = new Properties()
  val nodeProps: Properties = new Properties()

  nodeProps.setProperty("log-file", "snappyLeader.log")
  val logLevel: String = System.getProperty("logLevel", "config")
  nodeProps.setProperty("log-level", logLevel)
  if (logLevel.startsWith("fine") || logLevel == "all") {
    sysProps.setProperty("DistributionManager.VERBOSE", "true")
  }

  sysProps.setProperty("user.dir", Path(s"$workHome/$memberName").jfile.getAbsolutePath);
  println("setting user.dir="+sysProps.getProperty("user.dir"))
  nodeProps.setProperty("sys-disk-dir", s"$workHome/$memberName");
  nodeProps.setProperty("statistic-archive-file", s"$workHome/$memberName/snappyLeader.gfs")

  nodeProps.setProperty("name", memberName)
  nodeProps.setProperty("deploy-working-dir", s"$workHome/deploy")

  nodeProps.setProperty("security-log-level", System.getProperty("securityLogLevel", "config"))
  nodeProps.setProperty("statistic-archive-file", "snappyLeader.gfs")
  nodeProps.setProperty("spark.executor.cores","2");
    nodeProps.setProperty("spark.driver.memory","2g");
  nodeProps.setProperty("spark.memory.manager", "org.apache.spark.memory.SnappyUnifiedMemoryManager")



  val locatorPort: Int = 10334;
  val locatorBindClientAddress = InetAddress.getLocalHost.getHostAddress;

  def main(args: Array[String]): Unit = {
    setSystemProperties(sysProps)
    val node = ServiceManager.currentFabricServiceInstance
    if (node == null || node.status != FabricService.State.RUNNING) {
      println("start snapppy leader locatorPort: "+locatorPort);
      val workDir = System.getProperty("user.dir");
      //create work directory
      File(Path(workDir)).createDirectory(true, false)

      nodeProps.setProperty("locators", s"$locatorBindClientAddress[$locatorPort]")
      nodeProps.setProperty("log-level", "info")
      nodeProps.setProperty(Property.JobServerEnabled.name, "true")
      val server: Server = ServiceManager.getLeadInstance
      server.start(nodeProps)
      assert(server.status == FabricService.State.RUNNING)
    }
    assert(ServiceManager.currentFabricServiceInstance.status ==
      FabricService.State.RUNNING)

    waitForComplected
    logger.info("\n\n\n  STARTING IN " + getClass.getName + "\n\n")
  }

  def setSystemProperties(props: Properties): Unit = {
    val sysPropNames = props.stringPropertyNames().iterator()
    while (sysPropNames.hasNext) {
      val propName = sysPropNames.next()
      System.setProperty(propName, props.getProperty(propName))
    }
  }

  def waitForComplected: Unit = {
    val loop = new Breaks;
    loop.breakable(
      while (true){
//        println("waiting for locator end.")
        val currentLocator: FabricService = ServiceManager.currentFabricServiceInstance
        if(currentLocator.status != FabricService.State.STOPPED){
          try
            Thread.sleep(500L)
          catch {
            case ex: Exception =>
              ex.printStackTrace()
          }
        }else{
          loop.break()
        }
      }
    );
    logger.info("leader exit successful.")
    System.exit(0)
  }

}
