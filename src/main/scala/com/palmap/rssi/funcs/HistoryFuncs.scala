package com.palmap.rssi.funcs

import com.mongodb.{BasicDBObject, ServerAddress}
import com.mongodb.casbah.MongoClient
import com.palmap.rssi.common.{Common, GeneralMethods}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD
import org.json.JSONArray

import scala.collection.mutable.ListBuffer

/**
 * Created by admin on 2015/12/28.
 */
object HistoryFuncs {
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def saveHistory(rdd: RDD[(String, Array[Byte])], date: Long, currentTime: Long): Unit = {
    rdd.foreachPartition { partition => {
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
        val historyCollection = db(Common.MONGO_HISTORY)

        partition.foreach(record => {
          val visitor = Visitor.newBuilder().mergeFrom(record._2)
          val userMac = visitor.getPhoneMac
          val sceneId = visitor.getSceneId
          val locationId = visitor.getLocationId
          val findQuery = new BasicDBObject
          findQuery.put(Common.MONGO_HISTORY_DAYS, new BasicDBObject(Common.MONGO_OPTION_SLICE, List[Int](-1, 1)))

          val update = new BasicDBObject
          update.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_HISTORY_TIMES, 1))
          update.put(Common.MONGO_OPTION_PUSH, new BasicDBObject(Common.MONGO_HISTORY_DAYS, date))

          val query = new BasicDBObject
          query.put(Common.MONGO_HISTORY_LOCATIONID, locationId)
          query.put(Common.MONGO_HISTORY_SCENEID, visitor.getSceneId)
          query.put(Common.MONGO_HISTORY_MAC, new String(visitor.getPhoneMac.toByteArray()))

          var isDateExist = false
          val retList = historyCollection.find(query, findQuery).toList
          if (retList.size > 0) {
            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_DAYS).toString()).getLong(0)
            isDateExist = latestDate >= date
          }
          if (!isDateExist)
            historyCollection.update(query, update,  true)

        })
      } finally {
        mongoClient.close()
      }
    }
    }


  }

}
