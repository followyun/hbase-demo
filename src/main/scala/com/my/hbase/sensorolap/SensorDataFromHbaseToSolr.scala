package com.my.hbase.sensorolap

import java.util.UUID

import com.my.hbase.project.sensorolap.{CellEventConvertor, Event}
import com.my.hbase.utils.HBaseContext
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.{CellUtil, HBaseConfiguration, TableName}
import org.apache.spark.sql.types.{BinaryType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * 将Hbase中的sensor数据保存到solr上去
  */
object SensorDataFromHbaseToSolr {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SensorDataFromHbaseToSolr")
      .master("local").getOrCreate()

    val hbaseConf = HBaseConfiguration.create()
    val hbaseContext = new HBaseContext(spark.sparkContext, hbaseConf)

    val scan = new Scan()
    scan.setCaching(10)
    scan.setBatch(30)

    val sensorRDD = hbaseContext.hbaseRDD(TableName.valueOf("sensor"), scan)
    import scala.collection.JavaConversions._
    //转换为Row RDD
    val sensorRowRDD = sensorRDD.mapPartitions {
      case iterator =>
        val event = new Event()
        iterator.flatMap {
          case (_, result) =>
            val cells = result.listCells()
            cells.map {
              cell =>
                val finalEvent = new CellEventConvertor().cellToEvent(cell, event)
                Row(UUID.randomUUID().toString, CellUtil.cloneRow(cell), finalEvent.getEventType.toString, finalEvent.getPartName.toString)
            }

        }
    }
    val sensorSchema = StructType(
      StructField("id", StringType, false) ::
        StructField("rowkey", BinaryType, true) ::
        StructField("eventType", StringType, false) ::
        StructField("partName", StringType, false) :: Nil
    )
    val sensorDF = spark.createDataFrame(sensorRowRDD, sensorSchema)
    import com.lucidworks.spark.util.SolrDataFrameImplicits._
    val options = Map("zkhost" -> "master:2181,slave1:2181,slave2:2181/solr")
    sensorDF.write.options(options).solr("sensor")
    println("写入数据到solr成功！")
  }
}
