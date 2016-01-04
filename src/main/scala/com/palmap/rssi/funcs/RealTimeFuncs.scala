package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.{BasicDBObject, ServerAddress}
import com.mongodb.casbah.MongoClient
import com.palmap.rssi.common.{Common, GeneralMethods}
import com.palmap.rssi.message.ShopStore.Visitor
import com.palmap.rssi.message.Store.UserType
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Created by admin on 2015/12/28.
 */
object RealTimeFuncs {
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def calShop(record: (String, Array[Byte]), currentTime: Long): (String, (Long, Int, Int, String, Int)) = {
    val visitor = Visitor.newBuilder().mergeFrom(record._2)
    val userMac = new String(visitor.getPhoneMac.toByteArray())
    val sceneId = visitor.getSceneId
    val locationId = visitor.getLocationId
    val userType = visitor.getUserType
    (sceneId + Common.CTRL_A + locationId + Common.CTRL_A + userType, (currentTime, sceneId, locationId, userMac, userType))
  }

  def mergeMacs(partition: Iterator[(String, (Long, Int, Int, String, Int))]): Iterator[(String, (Long, Int, Int, Int, List[String]))] = {
    val ret = scala.collection.mutable.Map[String, (Long, Int, Int, Int, List[String])]()
    partition.foreach(record => {
      val userType = record._2._5
      if (ret.contains(record._1)) {
        val tmp = ret(record._1)
        ret += record._1 -> (tmp._1, tmp._2, tmp._3, record._2._5, (record._2._4) :: tmp._5)
      } else {
        ret += record._1 -> (record._2._1, record._2._2, record._2._3, record._2._5, List[String]())
      }

    })
    ret.toIterator
  }


  def saveMacs(rdd: RDD[(String, (Long, Int, Int, Int, List[String]))]): Unit = {
    //(currentTime,sceneId,locationId,userType,cusMacList,passingMacList)
    rdd.foreachPartition { partition =>
    {
      val mongoServerList = xmlConf(Common.MONGO_ADDRESS_LIST)
      val mongoServerArr = mongoServerList.split(",", -1)
      var serverList = ListBuffer[ServerAddress]()
      for (i <- 0 until mongoServerArr.length) {
        val server = new ServerAddress(mongoServerArr(i), xmlConf(Common.MONGO_SERVER_PORT).toInt)
        serverList.append(server)
      }
      val mongoClient = MongoClient(serverList.toList)

      try {
        val db = mongoClient(xmlConf(Common.MONGO_DB_NAME))
        val realTimeCollection = db(Common.MONGO_REALTIME)
        val realTimeHourCollection = db(Common.MONGO_REALTIMEHOUR)
        //.filter(record => ((!record._2._5.isEmpty) || (!record._2._6.isEmpty)))
        partition.foreach(record => {
          val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
          val minuteFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
          var currentDate = new Date(record._2._1)
          val hour = hourFormat.parse(hourFormat.format(currentDate)).getTime
          val createTime = minuteFormat.parse(minuteFormat.format(currentDate)).getTime

          val minQuery = new BasicDBObject(Common.MONGO_REALTIME_TIME, createTime)
          minQuery.put(Common.MONGO_REALTIME_SCENEID, record._2._2)
          minQuery.put(Common.MONGO_REALTIME_LOCATIONID, record._2._3)

          val hourQuery = new BasicDBObject(Common.MONGO_REALTIME_HOUR, hour)
          hourQuery.put(Common.MONGO_REALTIMEHOUR_SCENEID_, record._2._2)
          hourQuery.put(Common.MONGO_REALTIMEHOUR_LOCATIONID, record._2._3)
          //db.shop_realtime.ensureIndex({"time":1,"sceneId":1,"locationId":1})
          //db.shop_realtime.ensureIndex({"time":1,"sceneId":1,"locationId":1})

          val hourUpdate = new BasicDBObject
          val minUpdate = new BasicDBObject
          if (!record._2._5.isEmpty) {
            val macList = record._2._5
            val userType = record._2._4
            if (userType == UserType.CUSTOMER_VALUE) {
              minUpdate.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_REALTIME_CUSMAC_SUM, macList.size))
              minUpdate.put(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_REALTIME_CUSMACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
              hourUpdate.put(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_REALTIME_CUSMACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
            } else if (userType == UserType.PASSENGER_VALUE) {
              minUpdate.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_REALTIME_PASSINGMAC_SUM, macList.size))
              minUpdate.put(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_REALTIME_PASSINGMACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
              hourUpdate.put(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_REALTIME_PASSINGMACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
            }
          }

          realTimeCollection.update(minQuery, minUpdate, upsert = true)
          realTimeHourCollection.update(hourQuery, hourUpdate, upsert = true)

        })
      } finally {
        mongoClient.close()
      }
    }
    }
  }
  }
