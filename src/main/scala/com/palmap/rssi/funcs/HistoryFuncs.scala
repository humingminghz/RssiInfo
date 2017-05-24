package com.palmap.rssi.funcs

import com.mongodb.casbah.commons.MongoDBObject
import com.palmap.rssi.common.{Common, DateUtil, MongoFactory}
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
          var dayTime = DateUtil.getDayTimestamp(minuteTime)

          val queryBasic = MongoDBObject(Common.MONGO_HISTORY_SHOP_SCENE_ID -> sceneId,
            Common.MONGO_HISTORY_SHOP_MAC -> mac)
          val findBasic = MongoDBObject(Common.MONGO_HISTORY_SHOP_DAYS -> MongoDBObject(Common.MONGO_OPTION_SLICE -> List[Int](-1, 1)),
            Common.MONGO_HISTORY_SHOP_FIRST_DATE -> 1)
          val updateBasic = MongoDBObject(Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_HISTORY_SHOP_TIMES -> 1),
            Common.MONGO_OPTION_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_LAST_DATE -> dayTime),
            Common.MONGO_OPTION_ADD_TO_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_DAYS -> dayTime))

          val retList = historyCollection.find(queryBasic, findBasic).toList
          var isDateExist = false
          if (retList.nonEmpty) {
            val record = retList.head
            if (! record.containsField(Common.MONGO_HISTORY_SHOP_FIRST_DATE)) {
              val firstDayCol = MongoDBObject(Common.MONGO_HISTORY_SHOP_DAYS -> MongoDBObject(Common.MONGO_OPTION_SLICE -> 1))
              val firstDay = historyCollection.find(queryBasic, firstDayCol).toList
              if (firstDay.nonEmpty) {
                dayTime = new JSONArray(firstDay.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString).getLong(0)
                if (sceneId == 10062) println("sceneId: " + sceneId + "    firstTime: " + dayTime  +"  mac : "+ mac)
              }
              val firstUpdate = MongoDBObject(Common.MONGO_OPTION_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_FIRST_DATE -> dayTime))
              historyCollection.update(queryBasic, firstUpdate, upsert = true)
            }

            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString).getLong(0)
            isDateExist = latestDate >= dayTime
          }

          if (!isDateExist) {

            if (retList.isEmpty) {
              val firstUpdate = MongoDBObject(Common.MONGO_OPTION_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_FIRST_DATE -> dayTime))
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
