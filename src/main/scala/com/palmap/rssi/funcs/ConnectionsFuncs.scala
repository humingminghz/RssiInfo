package com.palmap.rssi.funcs

import com.mongodb.casbah.commons.MongoDBObject
import com.palmap.rssi.common.{Common, MongoFactory}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.control.NonFatal

/**
  * 保存链接数据到表shop_connections
  * @author Mingming.Hu 19.5.2017
  */
object ConnectionsFuncs {

  /**
    * 将计算结果保存至Common.MONGO_SHOP_CONNECTIONS表
    *
    * @param rdd RDD[sceneId + timeStamp, Set[Macs] ]
    */
   def saveConnections(rdd : RDD[(String, scala.collection.mutable.Set[String])]) : Unit = {
    try{
      rdd.foreachPartition( partition => {
        val col = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_CONNECTIONS)

        partition.foreach(record => {
          val keys = record._1.split(Common.CTRL_A, -1) // 分割key
          val sceneId = keys(0).toInt
          val timestamp = keys(1).toLong

          val queryBasic = MongoDBObject(Common.MONGO_SHOP_CONNECTIONS_SCENE_ID -> sceneId,
            Common.MONGO_SHOP_CONNECTIONS_TIME -> timestamp)
          val updateBasic = MongoDBObject(Common.MONGO_OPTION_SET -> MongoDBObject(Common.MONGO_SHOP_CONNECTIONS_IS_CUSTOMER -> false),
            Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_SHOP_CONNECTIONS_MAC_SUM -> record._2.size),
            Common.MONGO_OPTION_ADD_TO_SET -> MongoDBObject(Common.MONGO_SHOP_CONNECTIONS_MACS -> MongoDBObject(Common.MONGO_OPTION_EACH -> record._2)))

          col.update(queryBasic, updateBasic, upsert = true) // 更新
      })})

    }catch {
      case NonFatal(e) => println("Non Fatal Error happen, continue: " + e.printStackTrace())
      case e: InterruptedException => println("InterruptedException: " + e.printStackTrace())
    }

  }

  /**
    * 计算各个场景的macs列表
    *
    * @param partition Iterator[(SceneId + mac + timestamp, (rssiList, apList, isConnected))]
    * @return Iterator[(sceneId + timeStamp, macs)]
    */
  def calConnections(partition :Iterator[(String, (ArrayBuffer[Int],ArrayBuffer[Int], Boolean))]) : Iterator[(String, scala.collection.mutable.Set[String])] = {
    val resultList = ListBuffer[(String, scala.collection.mutable.Set[String])]()

    partition.foreach(record => {
      val keys = record._1.split(Common.CTRL_A, -1)
      val sceneId = keys(0).toInt
      val mac = keys(1)
      val timestamp = keys(2).toLong

      if(sceneId == Common.SCENE_ID_HUAWEI && record._2._3){ // 场景为华为 并且connected是true
        val macs = scala.collection.mutable.Set[String]()
        macs += mac.toUpperCase

        resultList += ((sceneId + Common.CTRL_A + timestamp, macs)) // 组成返回值
      }
    })

    resultList.toIterator
  }

}
