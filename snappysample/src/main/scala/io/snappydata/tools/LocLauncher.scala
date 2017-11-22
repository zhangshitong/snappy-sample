package io.snappydata.tools

import java.net.InetAddress
import java.util.Properties

import com.gemstone.gemfire.cache.CacheClosedException
import com.pivotal.gemfirexd.FabricService
import io.snappydata.tools.NodeLauncher.{memberName, nodeProps, sysProps, workHome}
import io.snappydata.{Locator, ServiceManager}
import org.slf4j.LoggerFactory

import scala.reflect.io.{File, Path}
import scala.util.control.Breaks

/**
  * Created by STZHANG on 2017/11/20.
  */
object LocLauncher {

  val workHome = "d:/apps/snappydata/work";
  val memberName = "locator1";


  val logger = LoggerFactory.getLogger(getClass)
  val clientAddress = InetAddress.getLocalHost.getHostAddress;
  val sysProps: Properties = new Properties()
  val locatorNetProps = new Properties()
  //locatorNetProps
  locatorNetProps.setProperty("name", memberName)
  locatorNetProps.setProperty("deploy-working-dir", s"$workHome/deploy/")
  locatorNetProps.setProperty("sys-disk-dir", s"$workHome/$memberName");
  locatorNetProps.setProperty("statistic-archive-file", s"$workHome/$memberName/snappyData.gfs")
  sysProps.setProperty("user.dir", Path(s"$workHome/$memberName").jfile.getAbsolutePath);

//  sysProps.setProperty("user.dir", "D:\\apps\\snappydata\\work\\locator");
  println("setting user.dir="+sysProps.getProperty("user.dir"))
  // reduce startup time
  sysProps.setProperty("p2p.discoveryTimeout", "1000")
  sysProps.setProperty("p2p.joinTimeout", "2000")
  sysProps.setProperty("p2p.minJoinTries", "1")

  val locatorPort: Int = 10334;
  val locatorNetPort: Int = 1527;


  def main(args: Array[String]): Unit = {
    val loc: Locator = ServiceManager.getLocatorInstance
    setSystemProperties(sysProps)

    println("user.dir="+System.getProperty("user.dir"))

    println("locator status: "+loc.status)
    if (loc.status != FabricService.State.RUNNING) {
      val workDir = System.getProperty("user.dir");
      //create work directory
      File(Path(workDir)).createDirectory(true, false)

      println(s"locator is starting on port:$locatorPort address: $clientAddress $clientAddress[$locatorPort]")
      loc.start(clientAddress, locatorPort, locatorNetProps)
      println("locator is start success.")
    }
    if (locatorNetPort > 0) {
      logger.info(s"starting network server on: $locatorNetPort")
      loc.startNetworkServer(clientAddress, locatorNetPort, locatorNetProps)
      logger.info(s"start network server on: $clientAddress:$locatorNetPort success.")
    }
    assert(loc.status == FabricService.State.RUNNING)

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
        val currentLocator: Locator = ServiceManager.getLocatorInstance
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
    logger.info("locator exit successful.")
    System.exit(0)
  }

}
