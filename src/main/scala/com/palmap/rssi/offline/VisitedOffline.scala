package com.palmap.rssi.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}
import com.palmap.rssi.statistic.ShopSceneFuncs

object VisitedOffline {

  val macBrandMap: Map[String, String] = ShopSceneFuncs.getMacBrandMap("mac_brand")

  def saveVisited( partition: Iterator[((Int, String), (List[Long], Boolean))]): Unit = {

    try {

      val visitorCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

      partition.foreach(record => {

        val sceneId = record._1._1
        val mac = record._1._2
        val timeList = record._2._1
        val minuteTime = timeList.head
        val  isCustomer=record._2._2

        val phoneMacKey = mac.substring(0, Common.MAC_KEY_LENGTH)
        var phoneBrand = Common.BRAND_UNKNOWN

        if (macBrandMap.contains(phoneMacKey)) {
          phoneBrand = macBrandMap(phoneMacKey)
        }

        val queryBasic = new BasicDBObject()
          .append(Common.MONGO_HISTORY_SHOP_SCENE_ID, sceneId)
          .append(Common.MONGO_HISTORY_SHOP_MAC, mac)

        val findBasic = new BasicDBObject()
          .append(Common.MONGO_OPTION_ID, 0)
          .append(Common.MONGO_HISTORY_SHOP_TIMES, 1)

        var times = 0
        val reList = historyCollection.find(queryBasic, findBasic).toList
        if (reList.nonEmpty) {
          times = reList.head.get(Common.MONGO_HISTORY_SHOP_TIMES).toString.toInt
        }

        val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
        val dayTime = sdf.parse(sdf.format(new Date(minuteTime))).getTime

        val queryVisit = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_DATE, dayTime)
          .append(Common.MONGO_SHOP_VISITED_SCENE_ID, sceneId)
          .append(Common.MONGO_SHOP_VISITED_MAC, mac)

        val updateBasic = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_TIMES, times)
          .append(Common.MONGO_SHOP_VISITED_IS_CUSTOMER, isCustomer)
          .append(Common.MONGO_SHOP_VISITED_PHONE_BRAND, phoneBrand)
          .append(Common.MONGO_SHOP_VISITED_DWELL, timeList.size)

        val updateCol = new BasicDBObject()
          .append(Common.MONGO_OPTION_SET, updateBasic)

        visitorCollection.update(queryVisit, updateCol, upsert = true)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
