package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.{WriteConcern, BasicDBObject}
import com.palmap.rssi.common.{Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD
import org.json.JSONArray

/**
 * Created by admin on 2015/12/28.
 */
object HistoryFuncs {
  def saveHistory(rdd: RDD[(String, Array[Byte])]): Unit = {

    rdd.foreachPartition(partition => {
      try {
        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
        partition.foreach(param => {
          val visitorBuilder = Visitor.newBuilder().mergeFrom(param._2, 0, param._2.length)
          val sceneId = visitorBuilder.getSceneId
          val mac = new String(visitorBuilder.getPhoneMac.toByteArray())
          val minuteTime = visitorBuilder.getTimeStamp
          println("HistoryFuncs mac: " + mac)
          val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          val dayTime = sdf.parse(sdf.format(new Date(minuteTime))).getTime

          val queryBasic = new BasicDBObject()
            .append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
            .append(Common.MONGO_HISTORY_SHOP_MAC, mac)

          val findBasic = new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, new BasicDBObject(Common.MONGO_OPTION_SLICE, List[Int](-1, 1)))

          val updateBasic = new BasicDBObject()
            .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_HISTORY_SHOP_TIMES, 1))
            .append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, dayTime))

          val retList = historyCollection.find(queryBasic, findBasic).toList
          var isDateExist = false
          if (retList.size > 0) {
            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString()).getLong(0)
            isDateExist = latestDate >= dayTime
          }

          if (!isDateExist) {
            historyCollection.update(queryBasic, updateBasic, true)
          }

        })
      } catch {
        case e: Exception => e.getStackTrace
      }

    })
  }


//  def saveHistory(rdd: RDD[(String, Array[Byte])]): Unit = {
//    rdd.foreachPartition { partition => {
//      try {
//        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
//        partition.foreach(record => {
//
//          val visitor = Visitor.newBuilder().mergeFrom(record._2)
//          val userMac = visitor.getPhoneMac
//          val sceneId = visitor.getSceneId
//          val currentTime = visitor.getTimeStamp
//          val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
//          val todayDate = todayDateFormat.format(new Date(currentTime))
//          val date = todayDateFormat.parse(todayDate).getTime
//
//          val update = new BasicDBObject
//          update.append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_HISTORY_SHOP_TIMES, 1))
//          update.append(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, date))
//
//          val query = new BasicDBObject
//          query.append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
//          query.append(Common.MONGO_HISTORY_SHOP_MAC, userMac)
//          val findQuery = new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, new BasicDBObject(Common.MONGO_OPTION_SLICE, List[Int](-1, 1)))
//
//          var isDateExist = false
//          val retList = historyCollection.find(query, findQuery).toList
//          if (retList.size > 0) {
//            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString()).getLong(0)
//            isDateExist = latestDate >= date
//          }
//
//          if (!isDateExist) {
//            historyCollection.update(query, update, true)
//          }
//
//        })
//      } catch {
//        case e: Exception => e.printStackTrace()
//      }
//    }
//    }
//
//
//  }

}
