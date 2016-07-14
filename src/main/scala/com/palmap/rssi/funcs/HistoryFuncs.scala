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
    try {
      rdd.foreachPartition(partition => {
        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
        partition.foreach(record => {
          val visitorBuilder = Visitor.newBuilder().mergeFrom(record._2, 0, record._2.length)
          val sceneId = visitorBuilder.getSceneId
          val mac = new String(visitorBuilder.getPhoneMac.toByteArray())
          val minuteTime = visitorBuilder.getTimeStamp
          val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          val dayTime = sdf.parse(sdf.format(new Date(minuteTime))).getTime

          val queryBasic = new BasicDBObject()
            .append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
            .append(Common.MONGO_HISTORY_SHOP_MAC, mac)

          val findBasic = new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, new BasicDBObject(Common.MONGO_OPTION_SLICE, List[Int](-1, 1)))

          val updateBasic = new BasicDBObject()
            .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_HISTORY_SHOP_TIMES, 1))
            .append(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_LASTDATE, dayTime).append(Common.MONGO_HISTORY_SHOP_FIRSTDATE, dayTime))
            .append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_DAYS, dayTime))

          val retList = historyCollection.find(queryBasic, findBasic).toList
          var isDateExist = false
          if (retList.size > 0) {
            // for compatibility
            val queryFirst = new BasicDBObject()
              .append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
              .append(Common.MONGO_HISTORY_SHOP_MAC, mac)
              .append(Common.MONGO_HISTORY_SHOP_FIRSTDATE, new BasicDBObject(Common.MONGO_OPTION_EXISTS, 1))
            val existsFirstDate = new BasicDBObject(Common.MONGO_HISTORY_SHOP_FIRSTDATE, 1)
            val firstDate = historyCollection.find(queryFirst, existsFirstDate).toList
            if (firstDate.size == 0) {
              val firstUpdate = new BasicDBObject(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_FIRSTDATE, dayTime))
              historyCollection.update(queryBasic, firstUpdate, true)
            }

            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString()).getLong(0)
            isDateExist = latestDate >= dayTime
          }

//          if (retList.size == 0) {
//            val firstUpdate = new BasicDBObject
//            historyCollection.update(queryBasic, firstUpdate, true)
//          }
//          historyCollection.update(queryBasic, updateBasic, true)

          if (!isDateExist) {
            if (retList.size == 0) {
              val firstUpdate = new BasicDBObject(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_HISTORY_SHOP_FIRSTDATE, dayTime))
              historyCollection.update(queryBasic, firstUpdate, true)
            }
            historyCollection.update(queryBasic, updateBasic, true)
          }

        })

      })
    }
    catch {
      case e: Exception => println( "ERROR  saveHistory: " + e.printStackTrace())
    }

  }


}
