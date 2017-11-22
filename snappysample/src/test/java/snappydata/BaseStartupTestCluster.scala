package snappydata

import io.snappydata.cluster.ClusterManagerTestBase

/**
  * Created by STZHANG on 2017/11/14.
  */
class BaseStartupTestCluster(s: String)  extends ClusterManagerTestBase(s) {
  val currenyLocatorPort = ClusterManagerTestBase.locPort
  def testStartCluster(): Unit={
     println("Cluster Startup.")
  }

  override def tearDown2(): Unit = {

  }

  override def afterClass(): Unit = {

  }
}
