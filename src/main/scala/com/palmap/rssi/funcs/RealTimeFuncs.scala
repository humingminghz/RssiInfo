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



  def saveMacs(record: (String, scala.collection.mutable.Set[String])): Unit = {
    try {
      val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIME)
      val realTimeHourCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIMEHOUR)
      val arrs = record._1.split(Common.CTRL_A, -1)
      val sceneId = arrs(0).toInt
      val isCustomer = arrs(1).toBoolean
      val macList = record._2
      val minTime=arrs(2).toLong
    /*  var currentDate = new Date(time)
      val currentSec = currentDate.getTime / 1000 * 1000
      currentDate = new Date(currentSec - Common.BATCH_INTERVAL_IN_MILLI_SEC)
      val minuteFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)*/
      val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
      val hour = hourFormat.parse(hourFormat.format(minTime)).getTime
      //val minTime = minuteFormat.parse(minuteFormat.format(currentDate)).getTime

      val query = new BasicDBObject(Common.MONGO_SHOP_REALTIME_TIME, minTime)
      query.put(Common.MONGO_SHOP_REALTIME_SCENEID, sceneId)
      query.put(Common.MONGO_SHOP_REALTIME_ISCUSTOMER, isCustomer)
      val update = new BasicDBObject()
      update.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACSUM, macList.size))
      update.put(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
      realTimeCollection.update(query, update, true)

      val hourQuery = new BasicDBObject(Common.MONGO_SHOP_REALTIME_HOUR, hour)
      hourQuery.put(Common.MONGO_SHOP_REALTIMEHOUR_SCENEID, sceneId)
      hourQuery.put(Common.MONGO_SHOP_REALTIME_ISCUSTOMER, isCustomer)
      val hourUpdate = new BasicDBObject
      hourUpdate.put(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_SHOP_REALTIMEHOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
      realTimeHourCollection.update(hourQuery, hourUpdate, true)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
