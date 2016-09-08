package com.palmap.rssi.statistic

import java.text.SimpleDateFormat
import java.util.Date

import com.google.protobuf.ByteString
import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{CommonConf, Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.FrostEvent.{IdType, RssiInfo, StubType}
import com.palmap.rssi.message.ShopStore.Visitor
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

object ShopUnitFuncs {

  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  val macBrandMap = CommonConf.macBrandMap
  val businessHoursMap = CommonConf.businessHoursMap
  val sceneIdlist = CommonConf.sceneIdlist

  //(sceneId + "," + apMac + "," + phoneMac + "," + createTime -> rssi)
//  def filterFuncs(record: (String, Int)): Boolean = {
//    val arr = record._1.split(",", -1)
//    val sceneId = arr(0).toInt
//    val time = arr(3).toLong
//
//    if (sceneIdlist.contains(sceneId)) {
//      if (!businessHoursMap.contains(sceneId)) {
//        return true
//      } else {
//        val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
//        val date = todayDateFormat.parse(todayDateFormat.format(time)).getTime
//        val openMinute = date + businessHoursMap(sceneId)._1 * Common.MINUTE_FORMATER
//        val closeMinute = date +businessHoursMap(sceneId)._2 * Common.MINUTE_FORMATER
//        return time >= openMinute && time <= closeMinute
//      }
//    } else {
//      return false
//    }
//  }


  def visitorInfo(visitor: Array[Byte]): mutable.HashMap[String, ArrayBuffer[Int]] = {
    val visitorMap = mutable.HashMap[String, ArrayBuffer[Int]]()
    val visitorBuilder = RssiInfo.newBuilder().mergeFrom(visitor, 0, visitor.length)
    if (visitorBuilder.hasSceneId && visitorBuilder.hasTimestamp && visitorBuilder.hasIdData && visitorBuilder.getIdType == IdType.MAC && visitorBuilder.getStubType == StubType.AP) {
      val timeStamp = visitorBuilder.getTimestamp
      val sceneId = visitorBuilder.getSceneId
      val sdf = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
      val dateStr = sdf.format(new Date(timeStamp))
      val minuteTime = sdf.parse(dateStr).getTime
      val currentDate = new Date()
      val currentTime = sdf.parse(sdf.format(currentDate)).getTime
      //过滤本批次要处理的数据
      if (minuteTime == (currentTime - Common.MINUTE_FORMATER)) {
        visitorBuilder.getItemsList.foreach(item => {
          if (item.hasIdData && item.hasRssi && item.getIdType == IdType.MAC) {
            val userMac = item.getIdData.toUpperCase()
            val rssi = item.getRssi
            val key = sceneId + Common.CTRL_A + userMac + Common.CTRL_A + minuteTime
            var rssiList = ArrayBuffer[Int]()
            if (visitorMap.contains(key)) {
              rssiList = visitorMap(key)
            }

            rssiList += rssi
            visitorMap += (sceneId  + Common.CTRL_A + userMac + Common.CTRL_A + minuteTime -> rssiList)
          }
        })

      }

    }
    visitorMap
  }

  /**
   * 营业时间过滤
   * @return
   */
  def filterBusinessVisitor(partition: Iterator[(String, ArrayBuffer[Int])]):Iterator[(String, ArrayBuffer[Int])] = {
    val recordList = ListBuffer[(String, ArrayBuffer[Int])]()
    partition.foreach(record => {
      val info = record._1.split(Common.CTRL_A, -1)
      val sceneId = info(0).toInt
      val minuteTime = info(2).toLong
      val mac = info(1)
      if (CommonConf.sceneIdlist.contains(sceneId)) {
        var isFlag = false
        if (CommonConf.businessHoursMap.contains(sceneId)) {
          val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          val date = todayDateFormat.parse(todayDateFormat.format(minuteTime)).getTime
          val openMinute = date + CommonConf.businessHoursMap(sceneId)._1 * Common.MINUTE_FORMATER
          val closeMinute = (date + CommonConf.businessHoursMap(sceneId)._2 * Common.MINUTE_FORMATER) - 1
//          println("sceneId: " + sceneId +"   openMinute: "+ openMinute +"   closeMinute:"+closeMinute +"   minuteTime: "+ minuteTime)
          isFlag = minuteTime >= openMinute && minuteTime <= closeMinute
        } else {
          isFlag = true
        }
        if(CommonConf.machineSet.contains(mac.toLowerCase())) {
          isFlag = false
        }

        if (isFlag) {
          recordList += record
        }
      }

    })
    recordList.toIterator
  }

  def buildVisitor(iter: Iterator[(String, ArrayBuffer[Int])]): Iterator[(String, Array[Byte])] = {
    val reList = ListBuffer[(String, Array[Byte])]()
    iter.foreach(event => {
      val keyInfo = event._1.split(Common.CTRL_A, -1)
      val sceneId = keyInfo(0).toInt
      val phoneMac = keyInfo(1).toLowerCase
      val minuteTime = keyInfo(2).toLong
      val rssiList = event._2.sorted
      val phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH)
      var phoneBrand = Common.BRAND_UNKNOWN
      if (macBrandMap.contains(phoneMacKey)) {
        phoneBrand = macBrandMap(phoneMacKey)
      }

      val visitor = Visitor.newBuilder()
        .setSceneId(sceneId)
        .setPhoneMac(ByteString.copyFrom(phoneMac.getBytes()))
        .setTimeStamp(minuteTime)
        .setIsCustomer(false)
        .setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes()))
        .addRssi(rssiList(rssiList.length - 1))
      reList += ((event._1, visitor.build().toByteArray))
    })
    reList.toIterator
  }

  def machineMacFilter(record: (String, Array[Byte])): Boolean = {
    if (record._2.isEmpty) return false
    val mac = record._1.split(Common.CTRL_A, -1)(1)
    !CommonConf.machineBrandSet.contains(mac.substring(0, 8))
  }

  def setIsCustomer(visitorIter: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {
    val shopRddMap = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()
    val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
    visitorIter.foreach(record => {
      val visitor = Visitor.newBuilder().clear().mergeFrom(record._2, 0, record._2.length)
      val userMac = new String(visitor.getPhoneMac.toByteArray())
      val sceneId = visitor.getSceneId

      val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
      val todayDate = todayDateFormat.format(new Date(visitor.getTimeStamp))
      val date = todayDateFormat.parse(todayDate).getTime

      val query = new BasicDBObject
      query.put(Common.MONGO_SHOP_VISITED_DATE, date)
      query.put(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
      query.put(Common.MONGO_SHOP_VISITED_MAC, userMac)

      val queryCol = new BasicDBObject(Common.MONGO_SHOP_VISITED_ISCUSTOMER, 1).append(Common.MONGO_OPTION_ID, 0)

      var isCustomer = false;
      val result = visitedCollection.find(query, queryCol).toList
      if (result.size > 0) {
        val ret = result.head
        isCustomer = ret.get(Common.MONGO_SHOP_VISITED_ISCUSTOMER).toString.toBoolean
      }
      visitor.setIsCustomer(isCustomer)
      shopRddMap.append((record._1, visitor.build().toByteArray()))
    })
    shopRddMap.toIterator
  }


  def checkMachine(partition: Iterator[(String,Boolean, Array[Byte])]): Iterator[String]  ={
    val ret = mutable.Set[String]()
    partition.foreach(record => {

      val keyInfo = record._1.split(Common.CTRL_A, -1)
      println(keyInfo(1))
      ret.add(keyInfo(1))
    })
    ret.iterator
  }

}