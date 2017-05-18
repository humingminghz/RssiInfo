package com.palmap.rssi.offline

import scala.collection.mutable

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, MongoFactory}

object MacFilterFuncs {

  //((sceneId, phoneMac）,dataDate)
  def checkMachine(partition: Iterator[((Int, String), Long)]): Iterator[String] = {

    val ret = mutable.Set[String]()

    try {

      val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)

      partition.foreach(record => {

        val sceneId = record._1._1
        val mac = record._1._2.toLowerCase()

        val dateList = (0 to 6).map(num => record._2 - num * Common.DAY_FORMATTER).toList
        val query = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_DATE, new BasicDBObject(Common.MONGO_OPTION_IN, dateList))
          .append(Common.MONGO_HISTORY_SHOP_SCENE_ID, sceneId)
          .append(Common.MONGO_HISTORY_SHOP_MAC, mac)

        val findQuery = new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, 1)

        val dwellList = visitedCollection.find(query, findQuery).toList

        if (dwellList.nonEmpty) {

          var machineTimes = 0

          dwellList.foreach(dwell => {
            if (dwell.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt >= Common.DEFAULT_MACHINE_CHECK_MINUTE)
              machineTimes += 1
          })

          if (machineTimes >= Common.DEFAULT_MACHINE_CHECK_TIMES) ret.add(record._1._2)
        }
      })

    } catch {
      case e: Exception => println("ERROR: " + e.getStackTraceString)
    }

    ret.toIterator
  }

}
