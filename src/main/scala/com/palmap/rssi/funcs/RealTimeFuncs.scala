package com.palmap.rssi.funcs

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import com.mongodb.casbah.commons.MongoDBObject
import com.palmap.rssi.common.{Common, DateUtil, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

/**
  * 更新realtime表信息
  */
object RealTimeFuncs {

  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)

  /**
    * 将场景ID isCustomer 和时间戳作为key mac列表作为value返回
    * @param partition 待处理partition
    * @return (sceneId + Common.CTRL_A + isCustomer + Common.CTRL_A + minuteTime, macs)
    */
  def calRealTime(partition: Iterator[(String, Array[Byte])]): Iterator[(String, scala.collection.mutable.Set[String])] = {

    val retList = ListBuffer[(String, scala.collection.mutable.Set[String])]()

    try {

      partition.foreach(iterator => {

        // 获取各种信息 为返回值做准备
        val visitor = Visitor.newBuilder().mergeFrom(iterator._2, 0, iterator._2.length)
        val sceneId = visitor.getSceneId
        val isCustomer = visitor.getIsCustomer
        val minuteTime = visitor.getTimeStamp

        val macs = scala.collection.mutable.Set[String]()
        macs += new String(visitor.getPhoneMac.toByteArray).toUpperCase

        // 组成返回值
        retList += ((sceneId + Common.CTRL_A + isCustomer + Common.CTRL_A + minuteTime, macs))
      })
    } catch {
      case e: Exception => println("ERROR  calRealTime: " + e.printStackTrace())
    }

    retList.toIterator
  }

  /**
    * 保存realtime表信息 分钟级和小时级的表
    * @param rdd 待处理rdd
    */
  def saveRealTime(rdd: RDD[(String, scala.collection.mutable.Set[String])]): Unit = {

    rdd.foreachPartition(partition => {

      try {
        // MongoDB collection
        val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME)
        val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME_HOUR)

        partition.foreach(record => {

          val ele = record._1.split(Common.CTRL_A, -1)
          val sceneId = ele(0).toInt
          val isCustomer = ele(1).toBoolean
          val minuteTime = ele(2).toLong
          val macs = record._2

          // 分钟级表查询条件
          val queryBasic = MongoDBObject(Common.MONGO_SHOP_REAL_TIME_TIME -> minuteTime,
            Common.MONGO_SHOP_REAL_TIME_SCENE_ID -> sceneId,
            Common.MONGO_SHOP_REAL_TIME_IS_CUSTOMER -> isCustomer)
          // 将mac数量以及mac内容更新至mongo
          val updateBasic = MongoDBObject(Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_SHOP_REAL_TIME_MAC_SUM -> macs.size),
            Common.MONGO_OPTION_ADD_TO_SET -> MongoDBObject(Common.MONGO_SHOP_REAL_TIME_MACS -> MongoDBObject(Common.MONGO_OPTION_EACH -> macs)))

          realTimeCollection.update(queryBasic, updateBasic, upsert = true)

          // 小时级表处理
          val hour = DateUtil.getHourTimestamp(minuteTime)
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

  /**
    * 已废弃
    * @param rdd
    */
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
            val hour = DateUtil.getHourTimestamp(minTime)

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
