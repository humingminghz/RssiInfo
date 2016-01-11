package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.MongoClient
import com.mongodb.{BasicDBObject, ServerAddress}
import com.palmap.rssi.common.{Common, GeneralMethods}
import com.palmap.rssi.message.ShopStore.Visitor
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
    (sceneId + Common.CTRL_A + locationId + Common.CTRL_A + userType , (currentTime, sceneId, locationId, userMac, userType))
  }

  def mergeMacs(partition: Iterator[(String, (Long, Int, Int, String, Int))]): Iterator[(String, (Long, Int, Int, Int, List[String]))] = {
    val ret = scala.collection.mutable.Map[String, (Long, Int, Int, Int, List[String])]()
    partition.foreach(record => {
      val userType = record._2._5
      if (ret.contains(record._1)) {
        val tmp = ret(record._1)
        ret += record._1 ->(tmp._1, tmp._2, tmp._3, record._2._5, (record._2._4) :: tmp._5)
      } else {
        val list = List[String]()
        ret += record._1 ->(record._2._1, record._2._2, record._2._3, record._2._5, (record._2._4) :: list)
      }

    })
    ret.toIterator
  }


    def saveMacs(rdd: RDD[(String, (Long, Int, Int, Int, List[String]))]): Unit = {

      //(currentTime,sceneId,locationId,userType,macList)
      rdd.foreachPartition { partition => {
        val mongoServerList = xmlConf(Common.MONGO_ADDRESS_LIST)
        val mongoServerArr = mongoServerList.split(",", -1)
        var serverList = ListBuffer[ServerAddress]()
        for (i <- 0 until mongoServerArr.length) {
          val server = new ServerAddress(mongoServerArr(i), xmlConf(Common.MONGO_SERVER_PORT).toInt)
          serverList.append(server)
        }
        val mongoClient = MongoClient(serverList.toList)
        val db = mongoClient(xmlConf(Common.MONGO_DB_NAME))
        val realTimeCollection = db(Common.MONGO_COLLECTION_REALTIME)
        val realTimeHourCollection = db(Common.MONGO_COLLECTION_REALTIME_HOUR)
        try {
          partition.filter(record => !record._2._5.isEmpty).foreach(record => {
            val minuteFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
            val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
            val currentDate = new Date(record._2._1)
            val hour = hourFormat.parse(hourFormat.format(currentDate)).getTime
            val minTime = minuteFormat.parse(minuteFormat.format(currentDate)).getTime
            //val macList = record._2._5
           // val list = List[String]()
            val macList = record._2._5
            /*.map(record => {
              record :: list
            })*/
            val minQuery = new BasicDBObject(Common.MONGO_REALTIME_TIME,minTime)
            minQuery.put(Common.MONGO_REALTIME_SCENEID, record._2._2)
            minQuery.put(Common.MONGO_REALTIME_LOCATIONID, record._2._3)
            minQuery.put(Common.MONGO_REALTIME_USERTYPE, record._2._4)
            val minUpdate = new BasicDBObject
            minUpdate.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_REALTIME_MACSUM, macList.size))
            minUpdate.put(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_REALTIME_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
            realTimeCollection.update(minQuery, minUpdate, true)
            //db.shop_realtime.ensureIndex({"time":1,"sceneId":1,"locationId":1,"userType":1})
            //db.shop_realtime.ensureIndex({"time":1,"sceneId":1,"locationId":1,"userType":1})

            val hourQuery = new BasicDBObject(Common.MONGO_REALTIME_HOUR, hour)
            hourQuery.put(Common.MONGO_REALTIME_HOUR_SCENEID, record._2._2)
            hourQuery.put(Common.MONGO_REALTIME_HOUR_LOCATIONID, record._2._3)
            hourQuery.put(Common.MONGO_REALTIME_HOUR_USERTYPE, record._2._4)
            val hourUpdate = new BasicDBObject
            hourUpdate.put(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_REALTIMEHOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
            realTimeHourCollection.update(hourQuery, hourUpdate, true)

          })
        } finally {
          mongoClient.close()
        }
      }
      }
    }

  def groupList(partition: (String, Iterable[(Long, Int, Int, String, Int)])): (String, (Long, Int, Int, Int,scala.collection.mutable.Set[String])) = {

    val arr = partition._1.split(Common.CTRL_A, -1)
    val sceneId = arr(0).toInt
    val locationId = arr(1).toInt
    val userType = arr(2).toInt
    var currentTime = (new Date()).getTime
    val macSet = scala.collection.mutable.Set[String]()
   // currentTime=partition._2.
    val iter = partition._2.iterator
    while (iter.hasNext) {
      val record = iter.next()
      val mac = record._4
      macSet.add(mac)
        if(currentTime>record._1)  currentTime=record._1

    }
    (partition._1, (currentTime, sceneId, locationId, userType, macSet))
  }

  def saveRealtimeRdd(rdd: RDD[(Long, Int, Int, Int, scala.collection.mutable.Set[String])]): Unit = {

    rdd.foreachPartition { partition => {
      val mongoServerList = xmlConf(Common.MONGO_ADDRESS_LIST)
      val mongoServerArr = mongoServerList.split(",", -1)
      var serverList = ListBuffer[ServerAddress]()
      for (i <- 0 until mongoServerArr.length) {
        val server = new ServerAddress(mongoServerArr(i), xmlConf(Common.MONGO_SERVER_PORT).toInt)
        serverList.append(server)
      }
      val mongoClient = MongoClient(serverList.toList)
      val db = mongoClient(xmlConf(Common.MONGO_DB_NAME))
      try {
        val testCollection = db("list_do")
       // db.list_do.ensureIndex({"time":1,"sceneId":1,"locationId":1,"userType":1})
        val realTimeCollection = db(Common.MONGO_COLLECTION_REALTIME)
        val realTimeHourCollection = db(Common.MONGO_COLLECTION_REALTIME_HOUR)
        partition.foreach(record => {
          var currentDate = new Date()
          val currentSec = currentDate.getTime / 1000 * 1000
          currentDate = new Date(currentSec - 60 * 1000)
          val minuteFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
          val hourFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
         // val currentDate = new Date(record._1)
          val hour = hourFormat.parse(hourFormat.format(currentDate)).getTime
          val minTime = minuteFormat.parse(minuteFormat.format(currentDate)).getTime

          val macList = record._5

          val query = new BasicDBObject(Common.MONGO_REALTIME_TIME,minTime )
          query.put(Common.MONGO_REALTIME_SCENEID, record._2)
          query.put(Common.MONGO_REALTIME_LOCATIONID, record._3)
          query.put(Common.MONGO_REALTIME_USERTYPE, record._4)
          val update = new BasicDBObject()
          update.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_REALTIME_MACSUM, macList.size))
          update.put(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_REALTIME_MACS,macList))
          realTimeCollection.update(query, update, true)

          val hourQuery = new BasicDBObject(Common.MONGO_REALTIME_HOUR, hour)
          hourQuery.put(Common.MONGO_REALTIME_HOUR_SCENEID, record._2)
          hourQuery.put(Common.MONGO_REALTIME_HOUR_LOCATIONID, record._3)
          hourQuery.put(Common.MONGO_REALTIME_HOUR_USERTYPE, record._4)
          val hourUpdate = new BasicDBObject
          hourUpdate.put(Common.MONGO_OPTION_ADDTOSET, new BasicDBObject(Common.MONGO_REALTIMEHOUR_MACS, new BasicDBObject(Common.MONGO_OPTION_EACH, macList)))
          realTimeHourCollection.update(hourQuery, hourUpdate, true)

        })
      } finally {
        mongoClient.close()
      }
    }

    }
  }




}
