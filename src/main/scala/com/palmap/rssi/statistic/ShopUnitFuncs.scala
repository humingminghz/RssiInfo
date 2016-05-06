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

  val macBrandMap = ShopSceneFuncs.getMacBrandMap("mac_brand")
  val businessHoursMap = CommonConf.businessHoursMap
  val sceneIdlist = CommonConf.sceneIdlist

  //(sceneId + "," + apMac + "," + phoneMac + "," + createTime -> rssi)
  def filterFuncs(record: (String, Int)): Boolean = {
    val arr = record._1.split(",", -1)
    val sceneId = arr(0).toInt
    val time = arr(3).toLong

    if (sceneIdlist.contains(sceneId)) {
      if (!businessHoursMap.contains(sceneId)) {
        return true
      } else {
        val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
        val date = todayDateFormat.parse(todayDateFormat.format(time)).getTime
        val openMinute = date + businessHoursMap(sceneId)._1 * Common.MINUTE_FORMATER
        val closeMinute = date +businessHoursMap(sceneId)._2 * Common.MINUTE_FORMATER
        return time >= openMinute && time <= closeMinute
      }
    } else {
      return false
    }
  }


  def visitorInfo1(visitor: Array[Byte]): Map[String, ArrayBuffer[Int]] = {
    val visitorBuilder = RssiInfo.newBuilder().mergeFrom(visitor, 0, visitor.length)
    var visitorMap = Map[String, ArrayBuffer[Int]]()
    if (visitorBuilder.hasSceneId && visitorBuilder.hasTimestamp && visitorBuilder.hasIdData && visitorBuilder.getIdType == IdType.MAC && visitorBuilder.getStubType == StubType.AP) {
      val sceneId = visitorBuilder.getSceneId
      val timeStamp = visitorBuilder.getTimestamp
      val sdf = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
      val dateStr = sdf.format(new Date(timeStamp))
      val minuteTime = sdf.parse(dateStr).getTime

      val currentDate = new Date()
      val currentTime = sdf.parse(sdf.format(currentDate)).getTime
      //过滤本批次要处理的数据
      if (minuteTime == (currentTime - Common.MINUTE_FORMATER)) {
        visitorBuilder.getItemsList.foreach(item => {
          if (item.hasIdData && item.hasRssi && item.getIdType == IdType.MAC) {
            val userMac = item.getIdData
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
  def filterBusinessVisitor(partition: Iterator[(String, ArrayBuffer[Int])]): Iterator[(String, ArrayBuffer[Int])] = {
    val recordList = ListBuffer[(String, ArrayBuffer[Int])]()
    partition.grouped(3)
    partition.foreach(record => {
      val info = record._1.split(Common.CTRL_A, -1)
      val sceneId = info(0).toInt
      val minuteTime = info(2).toLong
      val mac=info(1)
      recordList += record
//
//       if (CommonConf.sceneIdlist.contains(sceneId)) {
//        var isFlag = false
//        if (CommonConf.businessHoursMap.contains(sceneId)) {
//          val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
//          val date = todayDateFormat.parse(todayDateFormat.format(minuteTime)).getTime
//
//          val openMinute = date + CommonConf.businessHoursMap(sceneId)._1 * Common.MINUTE_FORMATER
//          val closeMinute = date + CommonConf.businessHoursMap(sceneId)._2 * Common.MINUTE_FORMATER
//          isFlag = minuteTime >= openMinute && minuteTime <= closeMinute
//
//        } else {
//          isFlag = true
//        }
//         if(CommonConf.machineSet.contains(mac.toLowerCase())){
//           isFlag = false
//
//         }
//
//         if (isFlag) {
//          recordList += record
//        }
//
//      }
     })

    recordList.toIterator
  }

  def buildVisitor1(iter: Iterator[(String, ArrayBuffer[Int])]): Iterator[(String, Array[Byte])] = {
    val reList = ListBuffer[(String, Array[Byte])]()

    iter.foreach(event => {
      val keyInfo = event._1.split(Common.CTRL_A, -1)
      val sceneId = keyInfo(0).toInt
      val phoneMac = keyInfo(1)
      val minuteTime = keyInfo(2).toLong
     // println( phoneMac + " ----------------- " + event._2)
      val rssiList = event._2.sorted
     // println("sorted ele: " + rssiList(rssiList.length - 1))

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


  def visitorInfo(event: Array[Byte]): Map[String, Int] = {
    val frostEvent = RssiInfo.newBuilder()
    val shopRddMap = scala.collection.mutable.Map[String, Int]()
    try {
      frostEvent.mergeFrom(event, 0, event.length)
      if (frostEvent.getStubType == StubType.AP && frostEvent.getIdType == IdType.MAC && frostEvent.hasSceneId && frostEvent.hasTimestamp) {
        val timeStamp = frostEvent.getTimestamp
        val minuteFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
        val createDate = new Date(timeStamp)
        val createTime = minuteFormat.parse(minuteFormat.format(createDate)).getTime

        val currentDate = new Date()
        val currentTime = minuteFormat.parse(minuteFormat.format(currentDate)).getTime
        if (createTime == (currentTime - Common.BATCH_INTERVAL_IN_MILLI_SEC)) {
          val sceneId = frostEvent.getSceneId
          val apMac = frostEvent.getIdData.toLowerCase
          frostEvent.getItemsList.foreach(item => {
            val phoneMac = item.getIdData.toLowerCase
            val rssi = item.getRssi
            shopRddMap += (sceneId + "," + apMac + "," + phoneMac + "," + createTime -> rssi)
          })
        }
      }
    } catch {
      case e: Exception => e.getStackTrace
    }

    shopRddMap.toMap
  }

  def buildVisitor(event: Iterator[(String, Int)]): Iterator[(String, Array[Byte])] = {

    val shopRddMap = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()
    event.foreach(visitor => {
      val arr = visitor._1.split(",", -1)
      val sceneId = arr(0).toInt
      val apMac = arr(1)
      val phoneMac = arr(2)
      val timeStamp = arr(3).toLong
      val rssi = visitor._2.toInt

      val phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH)
      var phoneBrand = Common.BRAND_UNKNOWN
      if (macBrandMap.contains(phoneMacKey)) {
        phoneBrand = macBrandMap(phoneMacKey)
      }

      val visitorBuilder = Visitor.newBuilder()
      visitorBuilder.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes))
      visitorBuilder.setSceneId(sceneId)
      visitorBuilder.setTimeStamp(timeStamp)
      visitorBuilder.setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes))
      visitorBuilder.addRssi(rssi)

      shopRddMap.append((phoneMac + Common.CTRL_A + sceneId, visitorBuilder.build().toByteArray()))

    })

    shopRddMap.toIterator
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