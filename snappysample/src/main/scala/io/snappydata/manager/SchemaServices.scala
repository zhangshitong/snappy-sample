package io.snappydata.manager

import java.sql.Types

import com.pivotal.gemfirexd.internal.engine.Misc
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{SnappySession, SparkSession}

/**
  * Created by STZHANG on 2017/11/20.
  */
object SchemaServices {

  private[this] val  querytabledata: Unit ={
    val spark: SparkSession = SparkSession
      .builder
      .appName("DataManagerSmartConnector")
      // It can be any master URL
      .master("local[4]")
      // snappydata.connection property enables the application to interact with SnappyData store
      .config("snappydata.connection", "192.168.56.2:1527")
      .config("jobserver.enabled", "false")
      .getOrCreate

    val snSession = new SnappySession(spark.sparkContext)
    val df = snSession.sql("select * from APP.PARTSUPP");
    df.take(100).foreach(s => println(s));

//    val catalog = Misc.getMemStore.getExternalCatalog
//    val meta = catalog.getHiveTableMetaData("APP", "PARTSUPP", false)
//    println(meta)
//
//    val meta2 = catalog.getColumnTableSchemaAsJson("APP", "PARTSUPP", false)
//    println(meta2)


  }



  private[this] val  querydataviajdbc: Unit ={
    import java.sql.DriverManager
    val conn = DriverManager.getConnection("jdbc:snappydata://192.168.56.2:1527/")
    val pstmt = conn.prepareStatement(s"select * from APP.PARTSUPP where PS_PARTKEY=?")
    pstmt.setInt(1, 2);
    val rs = pstmt.executeQuery();
    while (rs.next()){
       println(rs.getInt(1));
    }
    println("----------")
    val tableschema = getSchema(conn, "APP.PARTSUPP")

    println(tableschema)

  }

  import java.sql.{Connection, Date, Timestamp, Types}

  private[this] val getSchema  = (connection: Connection, table: String) => {
    val stmt = connection.prepareCall("CALL SYS.GET_COLUMN_TABLE_SCHEMA(?, ?, ?)")
    try {
      val (schemaName, tableName) = {
        if (table.contains(".")) {
          val indexOfDot = table.indexOf(".")
          (table.substring(0, indexOfDot), table.substring(indexOfDot + 1))
        } else {
          (connection.getSchema, table)
        }
      }
      stmt.setString(1, schemaName)
      stmt.setString(2, tableName)
      stmt.registerOutParameter(3, Types.CLOB)
      stmt.execute()
//      val str = (stmt.getString(3))
//      println("---------------"+str+"")
      DataType.fromJson(stmt.getString(3)).asInstanceOf[StructType]
    } finally {
      stmt.close()
    }
  }


  def main(args: Array[String]): Unit = {
     querytabledata;
    querydataviajdbc;
  }

}
