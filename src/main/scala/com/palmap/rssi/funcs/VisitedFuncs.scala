package com.palmap.rssi.funcs

import com.mongodb.casbah.MongoClient
import com.mongodb.{BasicDBObject, ServerAddress}
import com.palmap.rssi.common.{Common, GeneralMethods}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Created by admin on 2015/12/22.
 */
object VisitedFuncs {


  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def calcDwell(rdd: RDD[(String, Array[Byte])], currentDate: Long): Unit = {
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
      val db = mongoClient(xmlConf(Common.MONGO_DB_NAME))
      try {
        val visitedCollection = db(Common.MONGO_COLLECTION_VISITED)
        val historyCollection = db(Common.MONGO_COLLECTION_HISTORY)
        partition.foreach(record => {
          val visitor = Visitor.newBuilder().mergeFrom(record._2)
          val userMac = new String(visitor.getPhoneMac.toByteArray())
          val sceneId = visitor.getSceneId
          val locationId = visitor.getLocationId
          // var isCustomer = (visitor.getUserType == UserType.CUSTOMER_VALUE)

          val historyQuery = new BasicDBObject
          historyQuery.put(Common.MONGO_VISITED_LOCATIONID, locationId)
          historyQuery.put(Common.MONGO_VISITED_SCENEID, visitor.getSceneId)
          historyQuery.put(Common.MONGO_VISITED_MAC,userMac)
          //db.shop_history.ensureIndex({"sceneId":1,"locationId":1,"mac":1})
          val historyFind = new BasicDBObject
          historyFind.put(Common.MONGO_VISITED_TIMES, 1)

          var times = 0;
          val retList = historyCollection.find(historyQuery, historyFind).toList
          if (retList.size > 0) {
            val ret = retList.head
            times = ret.get(Common.MONGO_VISITED_TIMES).toString().toInt
          }
          //save
          val query = new BasicDBObject
          query.put(Common.MONGO_VISITED_DATE, currentDate)
          query.put(Common.MONGO_VISITED_LOCATIONID, locationId)
          query.put(Common.MONGO_VISITED_SCENEID, visitor.getSceneId)
          query.put(Common.MONGO_VISITED_MAC, userMac)
          //db.shop_visited.ensureIndex({"sceneId":1,"locationId":1,"mac":1,"date":1})
          val update = new BasicDBObject
          update.put(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_VISITED_TIMES, times).append(Common.MONGO_VISITED_PHONEBRAND, new String(visitor.getPhoneBrand.toByteArray())))
          update.put(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_VISITED_DWELL, 1))
          visitedCollection.update(query, update, true)

        })
      } finally {
        mongoClient.close()
      }

    }
    }
  }

}
