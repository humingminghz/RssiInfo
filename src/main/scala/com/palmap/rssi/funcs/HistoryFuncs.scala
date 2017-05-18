package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD
import org.json.JSONArray

object HistoryFuncs {

  def saveHistory(rdd: RDD[(String, Array[Byte])]): Unit = {

    try {

      rdd.foreachPartition(partition => {

        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

        partition.foreach(record => {

          val visitorBuilder = Visitor.newBuilder().mergeFrom(record._2, 0, record._2.length)
          val sceneId = visitorBuilder.getSceneId
          val mac = new String(visitorBuilder.getPhoneMac.toByteArray)
          val minuteTime = visitorBuilder.getTimeStamp
          val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          var dayTime = sdf.parse(sdf.format(new Date(minuteTime))).getTime

          val queryBasic = new BasicDBObject()
            .append(Common.MONGO_HISTORY_SHOP_SCENE_ID, sceneId)
            .append(Common.MONGO_HISTORY_SHOP_MAC, mac)

          val findBasic = new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS,
            new BasicDBObject(Common.MONGO_OPTION_SLICE, List[Int](-1, 1))).append(Common.MONGO_HISTORY_SHOP_FIRST_DATE, 1)

          val updateBasic = new BasicDBObject()
            .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_HISTORY_SHOP_TIMES, 1))
            .append(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_LAST_DATE, dayTime))
            .append(Common.MONGO_OPTION_ADD_TO_SET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, dayTime))

          val retList = historyCollection.find(queryBasic, findBasic).toList
          var isDateExist = false
          if (retList.nonEmpty) {
            val record = retList.head
            if (! record.containsField(Common.MONGO_HISTORY_SHOP_FIRST_DATE)) {
              val firstDayCol = new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS,
                new BasicDBObject(Common.MONGO_OPTION_SLICE, 1))
              val firstDay = historyCollection.find(queryBasic, firstDayCol).toList
              if (firstDay.nonEmpty) {
                dayTime = new JSONArray(firstDay.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString).getLong(0)
                if (sceneId == 10062) println("sceneId: " + sceneId + "    firstTime: " + dayTime  +"  mac : "+ mac)
              }
              val firstUpdate = new BasicDBObject(Common.MONGO_OPTION_SET,
                new BasicDBObject(Common.MONGO_HISTORY_SHOP_FIRST_DATE, dayTime))
              historyCollection.update(queryBasic, firstUpdate, upsert = true)
            }

            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString).getLong(0)
            isDateExist = latestDate >= dayTime
          }

          if (!isDateExist) {

            if (retList.isEmpty) {
              val firstUpdate = new BasicDBObject(Common.MONGO_OPTION_SET,
                new BasicDBObject(Common.MONGO_HISTORY_SHOP_FIRST_DATE, dayTime))
              historyCollection.update(queryBasic, firstUpdate, upsert = true)
            }

            historyCollection.update(queryBasic, updateBasic, upsert = true)
          }

        })

      })
    } catch {
      case e: Exception => println( "ERROR  saveHistory: " + e.printStackTrace())
    }
  }

}
