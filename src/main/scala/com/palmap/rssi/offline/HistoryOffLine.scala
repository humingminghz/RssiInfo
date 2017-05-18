package com.palmap.rssi.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}
import org.json.JSONArray

object HistoryOffLine {

  //((sceneId, phoneMac,p honeBrand）,(List[minuteTime],isCustomer))
  def saveHistory(partition: Iterator[((Int, String), (List[Long], Boolean))]): Unit = {

    try {

      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

      partition.foreach(record => {

        val sceneId = record._1._1
        val phoneMac = record._1._2
        val timeList = record._2._1
        val minuteTime = timeList.head

        val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
        val dayTime = sdf.parse(sdf.format(new Date(minuteTime))).getTime

        val queryBasic = new BasicDBObject()
          .append(Common.MONGO_HISTORY_SHOP_SCENE_ID, sceneId)
          .append(Common.MONGO_HISTORY_SHOP_MAC, phoneMac)

        val findBasic = new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS,
          new BasicDBObject(Common.MONGO_OPTION_SLICE, List[Int](-1, 1)))

        val updateBasic = new BasicDBObject()
          .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_HISTORY_SHOP_TIMES, 1))
          .append(Common.MONGO_OPTION_ADD_TO_SET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, dayTime))

        val retList = historyCollection.find(queryBasic, findBasic).toList
        var isDateExist = false

        if (retList.nonEmpty) {
          val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString).getLong(0)
          isDateExist = latestDate >= dayTime
        }

        if (!isDateExist) {
          historyCollection.update(queryBasic, updateBasic, upsert = true)
        }

      })
    } catch {
      case e: Exception => e.getStackTrace
    }

  }

  def saveDayInfo(partition: Iterator[(Int, Long, Int, Int)]): Unit = {

    try {

      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_DAY_INFO)

      partition.foreach(record => {

        val sceneId = record._1
        val date = record._2
        val count = record._3
        val dwell = record._4

        val queryBasic = new BasicDBObject()
          .append(Common.MONGO_HISTORY_SHOP_DAY_INFO_SCENE_ID, sceneId)
          .append(Common.MONGO_HISTORY_SHOP_DAY_INFO_DATE, date)

        val updateBasic = new BasicDBObject()
          .append(Common.MONGO_HISTORY_SHOP_DAY_INFO_COUNT, count)
          .append(Common.MONGO_HISTORY_SHOP_DAY_INFO_DWELL, dwell)

        val updateCol = new BasicDBObject()
          .append(Common.MONGO_OPTION_SET, updateBasic)

        historyCollection.update(queryBasic, updateCol, upsert = true)
      })

    } catch {
      case e: Exception => e.getStackTrace
    }
  }

}
