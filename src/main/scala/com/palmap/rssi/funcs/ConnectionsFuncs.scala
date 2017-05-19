package com.palmap.rssi.funcs

import com.mongodb.{BasicDBList, BasicDBObject}
import com.palmap.rssi.common.{Common, MongoFactory}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

/**
  * 保存链接数据到表shop_connections
  * @author Mingming.Hu 19.5.2017
  */
object ConnectionsFuncs extends LazyLogging {

  /**
    * 保存实时链接至表shop_connections
    *
    * @param rdd
    */
  def saveConnections(rdd : RDD[(String, (ArrayBuffer[Int],ArrayBuffer[Int], Boolean))]) : Unit = {
    logger.error(rdd.count().toString)
    rdd.foreachPartition( partition => {
      try{
        val col = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_TOTAL_CONNECTION)
        // 对所有记录循环 对mac进行更新或者插入数据
        partition.foreach(record => {
          val keys = record._1.split(Common.CTRL_A, -1)
          val sceneId = keys(0).toLong
          val mac = keys(1)
          val timeStamp = keys(2).toLong

          logger.error("before: " + record._1)
          logger.error(sceneId.toString)
          logger.error(record._2._3.toString)

          if(sceneId == Common.SCENE_ID_HUAWEI && record._2._3){
            logger.error("after: " + record._1)
            // 查询sceneId和分级时间点
            val queryBasic = new BasicDBObject()
              .append(Common.MONGO_SHOP_CONNECTIONS_SCENE_ID, sceneId)
              .append(Common.MONGO_SHOP_CONNECTIONS_TIME, timeStamp)
            val findBasic = new BasicDBObject() // 返回macs 集合
              .append(Common.MONGO_SHOP_CONNECTIONS_MACS, 1)
            val updateBasic = new BasicDBObject()
              .append(Common.MONGO_SHOP_CONNECTIONS_IS_CUSTOMER, false) // 暂时isCustomer为false 未使用
              .append(Common.MONGO_SHOP_CONNECTIONS_SCENE_ID, sceneId)
              .append(Common.MONGO_SHOP_CONNECTIONS_TIME, timeStamp)

            val result = col.find(queryBasic,findBasic).toList

            if(result != null && result.nonEmpty){ // 存在历史数据 则更新当前macs 及长度
              val macs  = result.head.get(Common.MONGO_SHOP_CONNECTIONS_MACS).asInstanceOf[BasicDBList]
              if(!macs.contains(mac)){
                macs.add(mac)
              }
              updateBasic.append(Common.MONGO_SHOP_CONNECTIONS_MACS, macs)
              updateBasic.append(Common.MONGO_SHOP_CONNECTIONS_MAC_SUM, macs.size())
            }else{ // 否则就新建mac 及长度
              updateBasic.append(Common.MONGO_SHOP_CONNECTIONS_MACS, Set(mac))
              updateBasic.append(Common.MONGO_SHOP_CONNECTIONS_MAC_SUM, 1)
            }

            col.update(queryBasic, updateBasic, upsert = true) // 更新
          }
        })
      }catch {
        case NonFatal(e) => println("Non Fatal Error happen, continue: " + e.printStackTrace())
        case e: InterruptedException => println("InterruptedException: " + e.printStackTrace())
      }
    })
  }
}
