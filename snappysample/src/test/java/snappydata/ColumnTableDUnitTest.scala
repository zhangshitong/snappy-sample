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
package snappydata

import java.io.File
import java.util

import scala.collection.JavaConverters._
import scala.util.Random
import com.gemstone.gemfire.internal.cache.{BucketRegion, PartitionedRegion}
import com.pivotal.gemfirexd.internal.engine.Misc
import io.snappydata.cluster.ClusterManagerTestBase
import io.snappydata.test.dunit.{SerializableCallable, SerializableRunnable}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.execution.columnar.impl.ColumnFormatRelation
import org.apache.spark.sql.{Row, SaveMode, SnappyContext}
import org.junit.Test

// scalastyle:off println
/**
 * Some basic column table tests.
 */
class ColumnTableDUnitTest(s: String) extends ClusterManagerTestBase(s) {

  val currenyLocatorPort = ClusterManagerTestBase.locPort
  // changing the test to such that batches are created
  // and looking for column table stats
  def testSNAP365_FetchRemoteBucketEntries(): Unit = {
    val snc = SnappyContext(sc)

    var data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3),
      Seq(4, 2, 3), Seq(5, 6, 7), Seq(2, 8, 3), Seq(3, 9, 0), Seq(3, 9, 3))
    1 to 1000 foreach { _ =>
      data = data :+ Seq.fill(3)(Random.nextInt)
    }
    val rdd = sc.parallelize(data, 3).map(
      s => new Data(s(0), s(1), s(2)))

    val dataDF = snc.createDataFrame(rdd)

    val p = Map("COLUMN_MAX_DELTA_ROWS" -> "1000")

    snc.createTable(tableName, "column", dataDF.schema, p)

    val tName = ColumnFormatRelation.columnBatchTableName(tableName.toUpperCase())
    // we don't expect any increase in put distribution stats
    val getTotalEntriesCount = new SerializableCallable[AnyRef] {
      override def call(): AnyRef = {
        val pr: PartitionedRegion =
          Misc.getRegionForTable("APP." + tName, true).asInstanceOf[PartitionedRegion]

        var bf, nbf = 0L;
        println("columnBatchTableName fullPath: "+pr.getFullPath);
        Misc.getGemFireCache.getAllRegions.toArray().foreach(r => {
          r match {
            case br2: BucketRegion =>
              if(br2.isRowBuffer){
                println("buket-region: " + br2.getFullPath + " isRowBuffer: " + br2.isRowBuffer + ", PartitionedRegion"+br2.getPartitionedRegion.getFullPath);
                bf += 1
              }
              if (br2.getPartitionedRegion.getFullPath.equals(pr.getFullPath))
                if (br2.isRowBuffer) {
                  println("buket-region: " + br2.getFullPath + " of " + br2.getPartitionedRegion.getFullPath);
                  println("isRowBuffer: " + br2.isRowBuffer + " regionSize: "+br2.getRegionSize);
                  bf += 1
                }else {
                  println("buket-region: " + br2.getFullPath + " Not rowBuffer" + " regionSize: "+br2.getRegionSize);
                  nbf += 1
                }
            case pr3:  PartitionedRegion =>
              println("getFullPath: "+pr3.getFullPath)
              println("ColumnBatchSize:" +  pr3.getColumnBatchSize + ";ColumnMaxDeltaRows:"+ pr3.getColumnMaxDeltaRows +";getColumnMinDeltaRows:"+ pr3.getColumnMinDeltaRows     );
              var buckets = Set.empty[Integer]
              0 to (pr3.getTotalNumberOfBuckets - 1) foreach { x =>
                buckets = buckets + x
              }
              val iter = pr3.getAppropriateLocalEntriesIterator(
                buckets.asJava, false, false, true, pr3, true)
              var count = 0
              while (iter.hasNext) {
                iter.next()
                //               println("fullpath "+pr3.getFullPath)
                //println("The Region"+(pr3.getFullPath)+ "\n data: "+count + "=" +iter.next)
                count = count + 1
              }
              println("The Region"+(pr3.getFullPath)+ "\n total count is " + count)

            case _ =>
          }

        });

        println( "BucketRegion of "+pr.getFullPath+" cf count:" + bf +" & nbf count: "+ nbf);
//        val columnBatchNamePr = Misc.getRegionByPath("/APP/"+tName).asInstanceOf[PartitionedRegion];
//        val columnBatchNamePrCount = columnBatchNamePr.allEntries().size();
//        println("columnBatchName entries count: "+columnBatchNamePrCount)

        println("totalBuckets: "+pr.getTotalNumberOfBuckets)
        var buckets = Set.empty[Integer]
        0 to (pr.getTotalNumberOfBuckets - 1) foreach { x =>
          buckets = buckets + x
        }
        val iter = pr.getAppropriateLocalEntriesIterator(
          buckets.asJava, false, false, true, pr, true)
        var count = 0
        while (iter.hasNext) {
          iter.next
          count = count + 1
        }
        println("The total count is " + count)
        Int.box(count)
      }
    }

    val getLocalEntriesCount = new SerializableCallable[AnyRef] {
      override def call(): AnyRef = {
        val pr: PartitionedRegion =
          Misc.getRegionForTable("APP." + tName, true).asInstanceOf[PartitionedRegion]
        val iter = pr.getAppropriateLocalEntriesIterator(
          pr.getDataStore.getAllLocalBucketIds, false, false, true, pr, false)
        var count = 0
        while (iter.hasNext) {
          iter.next
          count = count + 1
        }
        Int.box(count)
      }
    }

    dataDF.write.mode(SaveMode.Append).saveAsTable(tableName)
    println("------------Flush to table---------")
    ColumnFormatRelation.flushLocalBuckets(tableName);

    val totalCounts = Array(vm0, vm1, vm2).map(_.invoke(getTotalEntriesCount))
    assert(totalCounts(0) == totalCounts(1))
    assert(totalCounts(0) == totalCounts(2))

    val localCounts = Array(vm0, vm1, vm2).map(_.invoke(getLocalEntriesCount).asInstanceOf[Int])

    assert(totalCounts(0) == localCounts.sum)

    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 1008, s"Unexpected elements ${r.length}, expected=1008")

    snc.dropTable(tableName, ifExists = true)
    Array(vm0,vm1,vm2).foreach(_.invoke(classOf[ClusterManagerTestBase], "validateNoActiveSnapshotTX"))

    println("waiting for input:")
    System.in.read()
    getLogWriter.info("Successful")
  }


  private val tableName: String = "ColumnTable"
//  private val tableNameWithPartition: String = "ColumnTablePartition"

  val props = Map.empty[String, String]

  def startSparkJob(): Unit = {
    val snc = SnappyContext(sc)
    createTable(snc)
    verifyTableData(snc)
    dropTable(snc)
    getLogWriter.info("Successful")
  }

  def createTable(snc: SnappyContext,
      tableName: String = tableName,
      props: Map[String, String] = props): Unit = {
    val data = Seq(Seq(1, 2, 3), Seq(7, 8, 9), Seq(9, 2, 3), Seq(4, 2, 3), Seq(5, 6, 7))
    val rdd = sc.parallelize(data, data.length).map(s => new Data(s(0), s(1), s(2)))
    val dataDF = snc.createDataFrame(rdd)
    snc.createTable(tableName, "column", dataDF.schema, props)
    dataDF.write.format("column").mode(SaveMode.Append).saveAsTable(tableName)
  }

  def verifyTableData(snc: SnappyContext, tableName: String = tableName): Unit = {
    val result = snc.sql("SELECT * FROM " + tableName)
    val r = result.collect()
    assert(r.length == 5, s"Unexpected elements ${r.length}, expected=5")
  }

  def dropTable(snc: SnappyContext, tableName: String = tableName): Unit = {
    snc.dropTable(tableName, ifExists = true)
  }


}

case class TData(Key1: Int, Value: Int)

case class Data(col1: Int, col2: Int, col3: Int)

case class PartitionData(col1: Int, Value: String, other1: String, other2: String)

case class PartitionDataInt(col1: Int, Value: Int, other1: Int, other2: Int)
