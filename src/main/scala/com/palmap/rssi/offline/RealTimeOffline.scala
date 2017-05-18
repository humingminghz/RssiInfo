package com.palmap.rssi.offline

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}

// TODO 类中的两个方法或许可以提成一个
object RealTimeOffline {

  // (sceneId,minTine,isCustomer),HashSet[mac])
  def saveRealTime(partition: Iterator[((Int, Long, Boolean), scala.collection.mutable.Set[String])]): Unit = {

    try {

      val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME)

      partition.foreach(record => {

        val sceneId = record._1._1
        val minuteTime = record._1._2
        val isCustomer = record._1._3
        val macList = record._2

        val queryBasic = new BasicDBObject()
          .append(Common.MONGO_SHOP_REAL_TIME_TIME, minuteTime)
          .append(Common.MONGO_SHOP_REAL_TIME_SCENE_ID, sceneId)
          .append(Common.MONGO_SHOP_REAL_TIME_IS_CUSTOMER, isCustomer)

        val updateBasic = new BasicDBObject()
          .append(Common.MONGO_SHOP_REAL_TIME_MAC_SUM, macList.size)
          .append(Common.MONGO_SHOP_REAL_TIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList))

        realTimeCollection.update(queryBasic, updateBasic, upsert = true)
      })
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // (sceneId,hour,isCustomer),HashSet[mac])
  def saveRealHourTime(partition: Iterator[((Int, Long, Boolean), scala.collection.mutable.Set[String])]): Unit = {

    try {

      val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME_HOUR)

      partition.foreach(record => {

        val sceneId = record._1._1
        val hourTime = record._1._2
        val isCustomer = record._1._3
        val macList = record._2

        val queryHourBasic = new BasicDBObject()
          .append(Common.MONGO_SHOP_REAL_TIME_HOUR, hourTime)
          .append(Common.MONGO_SHOP_REAL_TIME_HOUR_SCENE_ID, sceneId)
          .append(Common.MONGO_SHOP_REAL_TIME_HOUR_IS_CUSTOMER, isCustomer)

        val updateMacBasic = new BasicDBObject()
          .append(Common.MONGO_OPTION_ADD_TO_SET, new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))

        realTimeHourCollection.update(queryHourBasic, updateMacBasic, upsert = true)
      })

    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

}
