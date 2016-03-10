package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD
import org.json.JSONArray

/**
 * Created by admin on 2015/12/28.
 */
object HistoryFuncs {
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def saveHistory(rdd: RDD[(String, Array[Byte])]): Unit = {
    rdd.foreachPartition { partition => {
      try {
        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

        partition.foreach(record => {
          val visitor = Visitor.newBuilder().mergeFrom(record._2)
          val userMac = visitor.getPhoneMac
          val sceneId = visitor.getSceneId
          val currentTime = visitor.getTimeStamp
          val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          val todayDate = todayDateFormat.format(new Date(currentTime))
          val date = todayDateFormat.parse(todayDate).getTime

          val update = new BasicDBObject
          update.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_HISTORY_SHOP_TIMES, 1))
          update.put(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, date))

          val query = new BasicDBObject
          query.put(Common.MONGO_HISTORY_SHOP_SCENEID, visitor.getSceneId)
          query.put(Common.MONGO_HISTORY_SHOP_MAC, new String(visitor.getPhoneMac.toByteArray()))

          val findQuery = new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, new BasicDBObject(Common.MONGO_OPTION_SLICE, List[Int](-1, 1)))

          var isDateExist = false
          val retList = historyCollection.find(query, findQuery).toList
          if (retList.size > 0) {
            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString()).getLong(0)
            isDateExist = latestDate >= date
          }

          if (!isDateExist) {
            historyCollection.update(query, update,  true)
          }

        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
    }


  }

}
