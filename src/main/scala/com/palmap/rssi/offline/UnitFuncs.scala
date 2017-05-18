package com.palmap.rssi.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.google.protobuf.ByteString
import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{MongoFactory, GeneralMethods, CommonConf, Common}
import com.palmap.rssi.message.FrostEvent.{StubType, IdType, RssiInfo}
import com.palmap.rssi.message.ShopStore.Visitor
import com.palmap.rssi.statistic.ShopSceneFuncs
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

object UnitFuncs {

  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)
  val businessHoursMap: mutable.Map[Int, (Int, Int)] = CommonConf.businessHoursMap
  val sceneIdList: mutable.Set[Int] = CommonConf.sceneIdlist

  /**
   * 获取文件数据
    *
   * @param visitor
   * @return
   */
  def visitorInfo(visitor: BytesWritable): Map[String,Int] = {

    val visitorBuilder = RssiInfo.newBuilder().mergeFrom(visitor.getBytes, 0, visitor.getLength)
    var visitorMap = Map[String, Int]()

    if (visitorBuilder.hasSceneId && visitorBuilder.hasTimestamp && visitorBuilder.hasIdData && visitorBuilder.getIdType == IdType.MAC && visitorBuilder.getStubType == StubType.AP) {

      val sceneId = visitorBuilder.getSceneId
      val timeStamp = visitorBuilder.getTimestamp
      val sdf = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
      val dateStr = sdf.format(new Date(timeStamp))
      val minuteTime = sdf.parse(dateStr).getTime

      visitorBuilder.getItemsList.foreach(item => {
        if (item.hasIdData && item.hasRssi && item.getIdType == IdType.MAC) {
          val userMac = item.getIdData
          val rssi = item.getRssi

          visitorMap += (sceneId  + Common.CTRL_A + userMac + Common.CTRL_A + minuteTime -> rssi)
        }
      })
    }

    visitorMap
  }

  /**
   * 营业时间过滤
   * @return
   */
  def filterFuncs(record: (String, Int)): Boolean = {

    val arr = record._1.split(Common.CTRL_A, -1)
    val sceneId = arr(0).toInt
    val time = arr(2).toLong

    if (sceneIdList.contains(sceneId)) {

      if (!businessHoursMap.contains(sceneId)) {
        true
      } else {
        val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
        val date = todayDateFormat.parse(todayDateFormat.format(time)).getTime
        val openMinute = date + businessHoursMap(sceneId)._1 * Common.MINUTE_FORMATTER
        val closeMinute = date + businessHoursMap(sceneId)._2 * Common.MINUTE_FORMATTER

        time >= openMinute && time <= closeMinute
      }
    } else {
      false
    }
  }

  /**
   * key
   * @param event
   * @return
   */
  def buildMessage(event:(String,Int)): String ={

    val keyInfo = event._1.split(Common.CTRL_A, -1)
    val sceneId = keyInfo(0).toInt
    val phoneMac = keyInfo(1)
    val minuteTime = keyInfo(2).toLong

    println(sceneId  + Common.CTRL_A + phoneMac + Common.CTRL_A + minuteTime)

    sceneId + Common.CTRL_A + phoneMac + Common.CTRL_A + minuteTime
  }

  def mergeVisitor(record: String): ((Int, String), Long) = {

    val keyInfo = record.split(Common.CTRL_A, -1)
    val sceneId = keyInfo(0).toInt
    val phoneMac = keyInfo(1)
    val minuteTime = keyInfo(2).toLong

    ((sceneId, phoneMac), minuteTime)
  }

  def seqVisitor(list: List[Long], values: (Long)): List[(Long)] = {
    values :: list
  }

  def combVisitor(list1: List[Long], list2: List[Long]): List[Long] = {
    list1 ::: list2
  }

  // ((sceneId,phoneMac,phoneBrand),minuteTime)
  def setIsCustomer(partition:Iterator[((Int, String), List[Long])]): Iterator[((Int, String), (List[Long], Boolean))] = {

    val retList = ListBuffer[((Int, String), (List[Long], Boolean))]()

    partition.foreach(event => {

      val timeSize = event._2.size
      var isCustomer = false

      if (timeSize > Common.CUSTOMER_JUDGE) {
        isCustomer = true
      }

      retList += ((event._1, (event._2, isCustomer)))
    })

    retList.toIterator
  }

  def minVisitor(partition: Iterator[((Int, String), (List[Long], Boolean))]): Iterator[((Int, Long, Boolean), String)] = {

    val retList = ListBuffer[((Int, Long, Boolean), String)]()

    partition.foreach(event => {

      val sceneId = event._1._1
      val mac = event._1._2
      val timeList = event._2._1
      val isCustomer = event._2._2

      for (time <- timeList) {
        retList += (((sceneId, time, isCustomer), mac))
      }

    })

    retList.toIterator
  }

  def seqMinFlow(set: mutable.HashSet[String], values: String): mutable.HashSet[String] = {
    set += values
  }

  def combineFlow(set1: mutable.HashSet[String], set2: mutable.HashSet[String]): mutable.HashSet[String] = {
    set1 ++ set2
  }

  def hourVisitor(partition: Iterator[((Int, Long, Boolean), String)]): Iterator[((Int, Long, Boolean), String)] = {

    val retList = ListBuffer[((Int, Long, Boolean), String)]()

    partition.foreach(event => {

      val sceneId = event._1._1
      val mac = event._2
      val minTime = event._1._2
      val isCustomer = event._1._3

      val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
      val hourTime = hourFormat.parse(hourFormat.format(new Date(minTime))).getTime

      retList += (((sceneId, hourTime, isCustomer), mac))
    })

    retList.toIterator
  }

  def mergeDayInfo(record:((String, (Int, Int)))): (Int, Long, Int, Int) = {

    val arr = record._1.split(Common.CTRL_A)
    val sceneId = arr(0).toInt
    val date = arr(1).toLong

    (sceneId, date, record._2._1, record._2._2 / record._2._1)
  }

}
