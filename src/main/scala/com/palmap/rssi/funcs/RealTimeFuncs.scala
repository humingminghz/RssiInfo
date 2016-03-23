package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.MongoClient
import com.mongodb.{BasicDBObject, ServerAddress}
import com.palmap.rssi.common.{MongoFactory, Common, GeneralMethods}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Created by admin on 2015/12/28.
 */
object RealTimeFuncs {
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def calRealTime(partition: Iterator[(String, Array[Byte])]): Iterator[(String, scala.collection.mutable.Set[String])] = {
    val ret = scala.collection.mutable.ListBuffer[(String, scala.collection.mutable.Set[String])]()
    partition.foreach(record => {
      val visitor = Visitor.newBuilder().mergeFrom(record._2, 0, record._2.length)
      val macs = scala.collection.mutable.Set[String]()
      val isCustomer = visitor.getIsCustomer
      val time=visitor.getTimeStamp
      macs.add(new String(visitor.getPhoneMac.toByteArray))

      ret.append((visitor.getSceneId + Common.CTRL_A + isCustomer+Common.CTRL_A +time, macs))
    })

    ret.toIterator
  }


  def saveRealtimeRdd(rdd: RDD[(String, scala.collection.mutable.Set[String])]): Unit = {

    rdd.foreachPartition { partition => {
      try {
        val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIME)
        val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIMEHOUR)
        partition.foreach(record => {

          val arrs = record._1.split(Common.CTRL_A, -1)
          val sceneId = arrs(0).toInt
          val isCustomer = arrs(1).toBoolean
          val macList = record._2
          val minTime=arrs(2).toLong

          val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
          val hour = hourFormat.parse(hourFormat.format(minTime)).getTime

          val query = new BasicDBObject(Common.MONGO_SHOP_REALTIME_TIME, minTime)
          query.append(Common.MONGO_SHOP_REALTIME_SCENEID, sceneId)
          query.append(Common.MONGO_SHOP_REALTIME_ISCUSTOMER, isCustomer)
          val update = new BasicDBObject()
          update.append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACSUM, macList.size))
          update.append(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
          // update.put(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
          realTimeCollection.update(query, update, true)
          //db.shop_realtime.ensureIndex({"sceneId":1,"time":1})
          //db.shop_realtime_hour.ensureIndex({"sceneId":1,"hour":1})
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
