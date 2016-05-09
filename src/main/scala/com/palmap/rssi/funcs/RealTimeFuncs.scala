package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Created by admin on 2015/12/28.
 */
object RealTimeFuncs {
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def calRealTime(partition: Iterator[(String, Array[Byte])]): Iterator[(String, scala.collection.mutable.Set[String])] = {
    val retList = ListBuffer[(String, scala.collection.mutable.Set[String])]()
    partition.foreach(iter => {
      val visitor = Visitor.newBuilder().mergeFrom(iter._2, 0, iter._2.length)
      val sceneId = visitor.getSceneId
      val isCustomer = visitor.getIsCustomer
      val minuteTime = visitor.getTimeStamp

      val macs = scala.collection.mutable.Set[String]()
      macs += new String(visitor.getPhoneMac.toByteArray)

      retList += ((sceneId + Common.CTRL_A + isCustomer + Common.CTRL_A + minuteTime, macs))

    })

    retList.toIterator
  }

  def saveRealTime(rdd: RDD[(String, scala.collection.mutable.Set[String])]): Unit = {
      rdd.foreachPartition(partition => {
        try {
          val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIME)
          val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIMEHOUR)

          partition.foreach(record => {
            val ele = record._1.split(Common.CTRL_A, -1)
            val sceneId = ele(0).toInt
            val isCustomer = ele(1).toBoolean
            val minuteTime = ele(2).toLong
            val macs = record._2

            val queryBasic = new BasicDBObject()
              .append(Common.MONGO_SHOP_REALTIME_TIME, minuteTime)
              .append(Common.MONGO_SHOP_REALTIME_SCENEID, sceneId)
              .append(Common.MONGO_SHOP_REALTIME_ISCUSTOMER, isCustomer)

            val updateBasic = new BasicDBObject()
              .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACSUM, macs.size))
              .append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macs)))

            realTimeCollection.update(queryBasic, updateBasic, true)

            val sdf = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
            val hour = sdf.parse(sdf.format(new Date(minuteTime))).getTime

            val queryHourBasic = new BasicDBObject()
              .append(Common.MONGO_SHOP_REALTIME_HOUR, hour)
              .append(Common.MONGO_SHOP_REALTIMEHOUR_SCENEID, sceneId)
              .append(Common.MONGO_SHOP_REALTIME_HOUR_ISCUSTOMER, isCustomer)

            val updateMacBasic = new BasicDBObject()
              .append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIMEHOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macs)))

            realTimeHourCollection.update(queryHourBasic, updateMacBasic, true)

          })
        }
        catch {
          case e: Exception => e.printStackTrace()
        }

      })
  }


//  def calRealTime(partition: Iterator[(String, Array[Byte])]): Iterator[(String, scala.collection.mutable.Set[String])] = {
//    val ret = scala.collection.mutable.ListBuffer[(String, scala.collection.mutable.Set[String])]()
//    partition.foreach(record => {
//      val visitor = Visitor.newBuilder().mergeFrom(record._2, 0, record._2.length)
//      val macs = scala.collection.mutable.Set[String]()
//      val isCustomer = visitor.getIsCustomer
//      val time=visitor.getTimeStamp
//      macs.add(new String(visitor.getPhoneMac.toByteArray))
//
//      ret.append((visitor.getSceneId + Common.CTRL_A + isCustomer+Common.CTRL_A +time, macs))
//    })
//
//    ret.toIterator
//  }


  def saveRealtimeRdd(rdd: RDD[(String, scala.collection.mutable.Set[String])]): Unit = {
    rdd.foreachPartition { partition => {
      try {
        val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIME)
        val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIMEHOUR)
        partition.foreach(record => {

          val arrs = record._1.split(Common.CTRL_A, -1)
          val sceneId = arrs(0).toInt
          val isCustomer = arrs(1).toBoolean
          val minTime=arrs(2).toLong
          val macList = record._2

          val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
          val hour = hourFormat.parse(hourFormat.format(minTime)).getTime

          val query = new BasicDBObject(Common.MONGO_SHOP_REALTIME_TIME, minTime)
          query.append(Common.MONGO_SHOP_REALTIME_SCENEID, sceneId)
          query.append(Common.MONGO_SHOP_REALTIME_ISCUSTOMER, isCustomer)

          val update = new BasicDBObject()
          update.append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACSUM, macList.size))
          update.append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))

          realTimeCollection.update(query, update, true)

          val hourQuery = new BasicDBObject(Common.MONGO_SHOP_REALTIME_HOUR, hour)
          hourQuery.append(Common.MONGO_SHOP_REALTIMEHOUR_SCENEID, sceneId)
          hourQuery.append(Common.MONGO_SHOP_REALTIME_HOUR_ISCUSTOMER, isCustomer)

          val hourUpdate = new BasicDBObject
          hourUpdate.append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIMEHOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))

          realTimeHourCollection.update(hourQuery, hourUpdate, true)

        })
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }

    }
  }
}
