package com.palmap.rssi.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Created by admin on 2016/4/18.
 */
object RealTimeOffline {


  // (sceneId,minTine,isCustomer),HashSet[mac])
  def saveRealTime(partition: Iterator[((Int,Long,Boolean), scala.collection.mutable.Set[String])]): Unit = {
    try {
        val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIME)
        partition.foreach(record => {
          val sceneId =record._1._1
          val minuteTime =record._1._2
          val isCustomer=record._1._3
          val macList =record._2

          val queryBasic = new BasicDBObject()
            .append(Common.MONGO_SHOP_REALTIME_TIME, minuteTime)
            .append(Common.MONGO_SHOP_REALTIME_SCENEID, sceneId)
            .append(Common.MONGO_SHOP_REALTIME_ISCUSTOMER, isCustomer)


          val updateBasic = new BasicDBObject()
            .append(Common.MONGO_SHOP_REALTIME_MACSUM, macList.size)
            .append(Common.MONGO_SHOP_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList))

          //  .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACSUM, macList.size))
          //  .append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))

          realTimeCollection.update(queryBasic, updateBasic, true)
        })
      }catch {
        case e: Exception => e.printStackTrace()
       }
  }

  // (sceneId,hour,isCustomer),HashSet[mac])
  def saveRealHourTime(partition: Iterator[((Int,Long,Boolean), scala.collection.mutable.Set[String])]): Unit = {
    try {
      val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIMEHOUR)

      partition.foreach(record => {
        val sceneId =record._1._1
        val hourTime =record._1._2
        val isCustomer=record._1._3
        val macList =record._2

        val queryHourBasic = new BasicDBObject()
          .append(Common.MONGO_SHOP_REALTIME_HOUR, hourTime)
          .append(Common.MONGO_SHOP_REALTIMEHOUR_SCENEID, sceneId)
          .append(Common.MONGO_SHOP_REALTIME_HOUR_ISCUSTOMER, isCustomer)

        val updateMacBasic = new BasicDBObject()
          .append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIMEHOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))

        realTimeHourCollection.update(queryHourBasic, updateMacBasic, true)
      })
    }catch {
      case e: Exception => e.printStackTrace()
    }

  }

}
