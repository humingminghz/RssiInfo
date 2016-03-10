package com.palmap.rssi.statistic

import java.text.SimpleDateFormat
import java.util.Date

import com.google.protobuf.ByteString
import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{CommonConf, Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.FrostEvent.{IdType, RssiInfo, StubType}
import com.palmap.rssi.message.ShopStore.Visitor
import scala.collection.JavaConversions._

object ShopUnitFuncs {

  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  val macBrandMap = ShopSceneFuncs.getMacBrandMap("mac_brand")
  val businessHoursMap=CommonConf.businessHoursMap

 def  visitorFilter(record:Array[Byte]): Boolean ={

    val visitor = Visitor.newBuilder().mergeFrom(record)
    val sceneId = visitor.getSceneId
    if (visitor.getSceneId == 0 ) return false
    if (!CommonConf.businessHoursMap.contains((sceneId))) return true

    val time = visitor.getTimeStamp
    val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
    val date = todayDateFormat.parse(todayDateFormat.format(time)).getTime
    val openMinute = date + CommonConf.businessHoursMap(sceneId)._1 * Common.MINUTE_FORMATER
    val closeMinute = date + CommonConf.businessHoursMap(sceneId)._2 * Common.MINUTE_FORMATER
    time >= openMinute && time <= closeMinute
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
    }
    catch {
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

  def setIsCustomer(visitorIter: Iterator[(String,  Array[Byte])]):Iterator[(String, Array[Byte])]={
    val shopRddMap = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()
    val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
    visitorIter.foreach(record => {
      val visitor = Visitor.newBuilder().clear().mergeFrom(record._2,0, record._2.length)
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

}