package com.palmap.rssi.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}

/**
 * Created by admin on 2016/4/18.
 */
object VisitedOffline {

  def saveVisited( partition: Iterator[((Int,String,String),(List[Long],Boolean))]): Unit = {
    try {
      val visitorCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
      partition.foreach(record => {
        val sceneId = record._1._1
        val mac = record._1._2
        val phoneBrand = record._1._3
        val timeList = record._2._1
        val minuteTime = timeList.head
        val  isCustomer=record._2._2

        val queryBasic = new BasicDBObject()
          .append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
          .append(Common.MONGO_HISTORY_SHOP_MAC, mac)

        val findBasic = new BasicDBObject()
          .append(Common.MONGO_OPTION_ID, 0)
          .append(Common.MONGO_HISTORY_SHOP_TIMES, 1)

        var times = 0
        val reList = historyCollection.find(queryBasic, findBasic).toList
        if (reList.size > 0) {
          times = reList.head.get(Common.MONGO_HISTORY_SHOP_TIMES).toString.toInt
        }

        val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
        val dayTime = sdf.parse(sdf.format(new Date(minuteTime))).getTime

        val queryVisit = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_DATE, dayTime)
          .append(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
          .append(Common.MONGO_SHOP_VISITED_MAC, mac)

        val findDwell = new BasicDBObject().append(Common.MONGO_SHOP_VISITED_DWELL, 1).append(Common.MONGO_OPTION_ID, 0)

        val updateBasic = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_TIMES, times)
          .append(Common.MONGO_SHOP_VISITED_ISCUSTOMER, isCustomer)
          .append(Common.MONGO_SHOP_VISITED_PHONEBRAND, phoneBrand)
          .append(Common.MONGO_SHOP_VISITED_DWELL, timeList.size)

        val updateCol = new BasicDBObject()
          .append(Common.MONGO_OPTION_SET, updateBasic)

        visitorCollection.update(queryVisit, updateCol, true)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
