package com.palmap.rssi.funcs

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.mongodb.casbah.commons.MongoDBObject
import com.palmap.rssi.common.{Common, DateUtil, GeneralMethods, MongoFactory}
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

          val queryBasic = MongoDBObject(Common.MONGO_SHOP_REAL_TIME_TIME -> minuteTime,
            Common.MONGO_SHOP_REAL_TIME_SCENE_ID -> sceneId,
            Common.MONGO_SHOP_REAL_TIME_IS_CUSTOMER -> isCustomer)
          val updateBasic = MongoDBObject(Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_SHOP_REAL_TIME_MAC_SUM -> macs.size),
            Common.MONGO_OPTION_ADD_TO_SET -> MongoDBObject(Common.MONGO_SHOP_REAL_TIME_MACS -> MongoDBObject(Common.MONGO_OPTION_EACH -> macs)))

          realTimeCollection.update(queryBasic, updateBasic, upsert = true)

          val hour = DateUtil.getHourTimeStamp(minuteTime)
          val queryHourBasic = MongoDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR -> hour,
            Common.MONGO_SHOP_REAL_TIME_HOUR_SCENE_ID -> sceneId,
            Common.MONGO_SHOP_REAL_TIME_HOUR_IS_CUSTOMER -> isCustomer)
          val updateMacBasic = MongoDBObject(Common.MONGO_OPTION_ADD_TO_SET ->
            MongoDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR_MACS -> MongoDBObject(Common.MONGO_OPTION_EACH -> macs)))

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
            val hour = DateUtil.getHourTimeStamp(minTime)

            val query = MongoDBObject(Common.MONGO_SHOP_REAL_TIME_TIME -> minTime,
              Common.MONGO_SHOP_REAL_TIME_SCENE_ID -> sceneId,
              Common.MONGO_SHOP_REAL_TIME_IS_CUSTOMER -> isCustomer)
            val update = MongoDBObject(Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_SHOP_REAL_TIME_MAC_SUM -> macList.size),
              Common.MONGO_OPTION_ADD_TO_SET -> MongoDBObject(Common.MONGO_SHOP_REAL_TIME_MACS -> MongoDBObject(Common.MONGO_OPTION_EACH -> macList)))

            realTimeCollection.update(query, update, upsert = true)

            val hourQuery = MongoDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR -> hour,
              Common.MONGO_SHOP_REAL_TIME_HOUR_SCENE_ID -> sceneId,
              Common.MONGO_SHOP_REAL_TIME_HOUR_IS_CUSTOMER -> isCustomer)
            val hourUpdate = MongoDBObject(Common.MONGO_OPTION_ADD_TO_SET ->
              MongoDBObject(Common.MONGO_SHOP_REAL_TIME_HOUR_MACS -> MongoDBObject(Common.MONGO_OPTION_EACH -> macList)))

            realTimeHourCollection.update(hourQuery, hourUpdate, upsert = true)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }

    }
  }
}
