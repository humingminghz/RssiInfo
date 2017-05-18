package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

object RealTimeFuncs {

  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def calRealTime(partition: Iterator[(String, Array[Byte])]): Iterator[(String, scala.collection.mutable.Set[String])] = {

    val retList = ListBuffer[(String, scala.collection.mutable.Set[String])]()

    try {

      partition.foreach(iterator => {

        val visitor = Visitor.newBuilder().mergeFrom(iterator._2, 0, iterator._2.length)
        val sceneId = visitor.getSceneId
        val isCustomer = visitor.getIsCustomer
        val minuteTime = visitor.getTimeStamp

        val macs = scala.collection.mutable.Set[String]()
        macs += new String(visitor.getPhoneMac.toByteArray).toUpperCase

        retList += ((sceneId + Common.CTRL_A + isCustomer + Common.CTRL_A + minuteTime, macs))
      })
    } catch {
      case e: Exception => println("ERROR  calRealTime: " + e.printStackTrace())
    }

    retList.toIterator
  }

  def saveRealTime(rdd: RDD[(String, scala.collection.mutable.Set[String])]): Unit = {

    rdd.foreachPartition(partition => {

      try {

        val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME)
        val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME_HOUR)

        partition.foreach(record => {

          val ele = record._1.split(Common.CTRL_A, -1)
          val sceneId = ele(0).toInt
          val isCustomer = ele(1).toBoolean
          val minuteTime = ele(2).toLong
          val macs = record._2

          val queryBasic = new BasicDBObject()
            .append(Common.MONGO_SHOP_REAL_TIME_TIME, minuteTime)
            .append(Common.MONGO_SHOP_REAL_TIME_SCENE_ID, sceneId)
            .append(Common.MONGO_SHOP_REAL_TIME_IS_CUSTOMER, isCustomer)

          val updateBasic = new BasicDBObject()
            .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_MAC_SUM, macs.size))
            .append(Common.MONGO_OPTION_ADD_TO_SET,
              new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macs)))

          realTimeCollection.update(queryBasic, updateBasic, upsert = true)

          val sdf = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
          val hour = sdf.parse(sdf.format(new Date(minuteTime))).getTime

          val queryHourBasic = new BasicDBObject()
            .append(Common.MONGO_SHOP_REAL_TIME_HOUR, hour)
            .append(Common.MONGO_SHOP_REAL_TIME_HOUR_SCENE_ID, sceneId)
            .append(Common.MONGO_SHOP_REAL_TIME_HOUR_IS_CUSTOMER, isCustomer)

          val updateMacBasic = new BasicDBObject()
            .append(Common.MONGO_OPTION_ADD_TO_SET, new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macs)))

          realTimeHourCollection.update(queryHourBasic, updateMacBasic, upsert = true)
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }

    })
  }

  def saveRealTimeRdd(rdd: RDD[(String, scala.collection.mutable.Set[String])]): Unit = {

    rdd.foreachPartition {

      partition => {

        try {

          val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME)
          val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME_HOUR)
          partition.foreach(record => {

            val arr = record._1.split(Common.CTRL_A, -1)
            val sceneId = arr(0).toInt
            val isCustomer = arr(1).toBoolean
            val minTime = arr(2).toLong
            val macList = record._2

            val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
            val hour = hourFormat.parse(hourFormat.format(minTime)).getTime

            val query = new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_TIME, minTime)
            query.append(Common.MONGO_SHOP_REAL_TIME_SCENE_ID, sceneId)
            query.append(Common.MONGO_SHOP_REAL_TIME_IS_CUSTOMER, isCustomer)

            val update = new BasicDBObject()
            update.append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_MAC_SUM, macList.size))
            update.append(Common.MONGO_OPTION_ADD_TO_SET,
              new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))

            realTimeCollection.update(query, update, upsert = true)

            val hourQuery = new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR, hour)
            hourQuery.append(Common.MONGO_SHOP_REAL_TIME_HOUR_SCENE_ID, sceneId)
            hourQuery.append(Common.MONGO_SHOP_REAL_TIME_HOUR_IS_CUSTOMER, isCustomer)

            val hourUpdate = new BasicDBObject
            hourUpdate.append(Common.MONGO_OPTION_ADD_TO_SET,
              new BasicDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))

            realTimeHourCollection.update(hourQuery, hourUpdate, upsert = true)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

    }
  }
}
