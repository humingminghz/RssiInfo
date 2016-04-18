package com.palmap.rssi.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD
import org.json.JSONArray

/**
 * Created by admin on 2016/4/18.
 */
object HistoryOffLine {

  //((sceneId, phoneMac,phoneBrand）,(List[minuteTime],isCustomer))
  def saveHistory( partition: Iterator[((Int,String,String),(List[Long],Boolean))]): Unit = {
      try {
        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
        partition.foreach(  record => {
          val sceneId =record._1._1
          val phoneMac =record._1._2
          val phoneBrand=record._1._3
          val timeList=record._2._1
          val minuteTime =timeList.head

          println("HistoryFuncs mac: " + phoneMac)
          val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          val dayTime = sdf.parse(sdf.format(new Date(minuteTime))).getTime

          val queryBasic = new BasicDBObject()
            .append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
            .append(Common.MONGO_HISTORY_SHOP_MAC, phoneMac)

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

    }


}
