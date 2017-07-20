package com.palmap.rssi.funcs

import scala.collection.mutable.ListBuffer
import com.mongodb.casbah.commons.MongoDBObject
import com.palmap.rssi.common.{Common, CommonConf, DateUtil, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

/**
  * 更新visited表数据 day 级别数据
  */
object VisitedFuncs {

  /**
    * 计算停留时间
    * @param iterator 待处理数据
    * @return
    */
  def calVisitorDwell(iterator: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {

    val retList = ListBuffer[(String, Array[Byte])]()

    try {
      // mongoDB中collection
      val visitorCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
      val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REAL_TIME)

      iterator.foreach(item => {

        // 根据protobuf中visitor 获取相关信息
        val visitorBuilder = Visitor.newBuilder().mergeFrom(item._2, 0, item._2.length)
        val sceneId = visitorBuilder.getSceneId
        val mac = new String(visitorBuilder.getPhoneMac.toByteArray).toUpperCase
        val timestamp = visitorBuilder.getTimeStamp
        val phoneBrand = new String(visitorBuilder.getPhoneBrand.toByteArray)

        // 组成查询条件
        val queryBasic = MongoDBObject(Common.MONGO_HISTORY_SHOP_SCENE_ID -> sceneId,
          Common.MONGO_HISTORY_SHOP_MAC -> mac.toLowerCase)
        val findBasic = MongoDBObject(Common.MONGO_OPTION_ID -> 0,
          Common.MONGO_HISTORY_SHOP_FIRST_DATE -> 1,
          Common.MONGO_HISTORY_SHOP_LAST_DATE -> 1,
          Common.MONGO_HISTORY_SHOP_TIMES -> 1)

        //历史查询,获取到访次数
        val reList = historyCollection.find(queryBasic, findBasic).toList
        var times = 0
        var freq: Int = 0

        if (reList.nonEmpty) { // 以前来过

          val record = reList.head
          times = record.get(Common.MONGO_HISTORY_SHOP_TIMES).toString.toInt
          val firstDate = record.get(Common.MONGO_HISTORY_SHOP_FIRST_DATE).toString.toLong
          val lastDate = record.get(Common.MONGO_HISTORY_SHOP_LAST_DATE).toString.toLong

          // 计算频率
          if (times > 1) {
            freq = ((lastDate - firstDate) / Common.DAY_FORMATTER).toInt / (times - 1)
          }

        }
        // 取day级别数据
        val dayTime = DateUtil.getDayTimestamp(timestamp)

        // 组成查询条件
        val realQuery = MongoDBObject(Common.MONGO_SHOP_REAL_TIME_SCENE_ID -> sceneId,
          Common.MONGO_SHOP_REAL_TIME_MACS -> mac,
          Common.MONGO_SHOP_REAL_TIME_TIME -> MongoDBObject(Common.MONGO_OPTION_GTE -> (timestamp - Common.HOUR_FORMATTER)))
        val realFind = MongoDBObject(Common.MONGO_OPTION_ID -> 0,
          Common.MONGO_SHOP_REAL_TIME_TIME -> 1)

        //查询前一次的时间
        val minuteList = realTimeCollection.find(realQuery, realFind).sort(MongoDBObject(Common.MONGO_SHOP_REAL_TIME_TIME -> -1)).limit(1).toList
        var minuteAgo = 0L

        if (minuteList.nonEmpty) {
          minuteAgo = minuteList.head.get(Common.MONGO_SHOP_REAL_TIME_TIME).toString.toLong
        }

        //间隔时间(分钟)
        val intervalTime = ((timestamp - minuteAgo) / Common.MINUTE_FORMATTER).toInt
        var dwell = 1
        //判定小于等于10分钟的，dwell进行累计； 否则，判定为2次进店或路过，dwell为1
        if (intervalTime <= Common.INTERVAL_MINUTE) {
          dwell = intervalTime
        }

        // visited表查询条件
        val queryVisit = MongoDBObject(Common.MONGO_SHOP_VISITED_DATE -> dayTime,
          Common.MONGO_SHOP_VISITED_SCENE_ID -> sceneId,
          Common.MONGO_SHOP_VISITED_MAC -> mac)
        val findDwell = MongoDBObject(Common.MONGO_SHOP_VISITED_DWELL -> 1,
          Common.MONGO_OPTION_ID -> 0)

        val reDwell = visitorCollection.find(queryVisit, findDwell).toList
        var isCustomer = false

        // 根据之前停留之间 和停留时间和 计算isCustomer
        if (reDwell.nonEmpty && reDwell.head.containsField(Common.MONGO_SHOP_VISITED_DWELL)) {

          val dwellAgo = reDwell.head.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt

          if (CommonConf.sceneIdMap.contains(sceneId)) {
            val setDwell = CommonConf.sceneIdMap.getOrElse(sceneId, 5) // 读场景停留时间下限 判断isCustomer
            isCustomer = dwellAgo + dwell > setDwell
          } else if (sceneId == 10062) { // TODO: 10062是？ 特定SceneId修改成Common类的常量
            isCustomer = !phoneBrand.equals(Common.BRAND_UNKNOWN)
          } else {
            isCustomer = dwellAgo + dwell > 5
          }

        }

        if (CommonConf.sceneIdMap.get(sceneId) == Some(0)) {
          val setDwell = CommonConf.sceneIdMap.getOrElse(sceneId, 5)
          isCustomer = dwell > setDwell
        }

        //更新visited表
        val updateBasic = MongoDBObject(Common.MONGO_SHOP_VISITED_TIMES -> times,
          Common.MONGO_SHOP_VISITED_FREQUENCY -> freq,
          Common.MONGO_SHOP_VISITED_IS_CUSTOMER -> isCustomer,
          Common.MONGO_SHOP_VISITED_PHONE_BRAND -> phoneBrand)
        val updateCol = MongoDBObject(Common.MONGO_OPTION_SET -> updateBasic,
          Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_SHOP_VISITED_DWELL -> dwell))

        visitorCollection.update(queryVisit, updateCol, upsert = true)
        visitorBuilder.setIsCustomer(isCustomer)

        retList += ((item._1, visitorBuilder.build().toByteArray))
      })
    } catch {
      case e: Exception => println("ERROR calVisitorDwell: " + e.printStackTrace())
    }

    retList.toIterator
  }

  /**
    * 已废弃 待删
    * @param iterator
    * @return
    */
  def calcDwellIsCustomer(iterator: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {

    val reList = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()

    try {

      val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

      iterator.foreach(record => {

        val visitor = Visitor.newBuilder().mergeFrom(record._2)
        val userMac = new String(visitor.getPhoneMac.toByteArray)
        val sceneId = visitor.getSceneId

        val historyQuery = MongoDBObject(Common.MONGO_SHOP_VISITED_SCENE_ID -> sceneId,
         Common.MONGO_SHOP_VISITED_MAC -> userMac)
        val historyFind = MongoDBObject(Common.MONGO_SHOP_VISITED_TIMES -> 1)

        val retList = historyCollection.find(historyQuery, historyFind).toList

        var times = 0
        if (retList.nonEmpty) {
          val ret = retList.head
          times = ret.get(Common.MONGO_SHOP_VISITED_TIMES).toString.toInt
        }

        val date = DateUtil.getDayTimestamp(visitor.getTimeStamp)

        //save
        val query = MongoDBObject(Common.MONGO_SHOP_VISITED_DATE -> date,
          Common.MONGO_SHOP_VISITED_SCENE_ID -> sceneId,
          Common.MONGO_SHOP_VISITED_MAC -> userMac)

        //db.shop_visited.ensureIndex({"date":1,"sceneId":1,"mac":1})
        val queryCol = MongoDBObject(Common.MONGO_SHOP_VISITED_DWELL -> 1,
          Common.MONGO_OPTION_ID -> 0)

        var isCustomer = false
        val dwellRet = visitedCollection.find(query, queryCol).toList

        if (dwellRet.nonEmpty && dwellRet.head.containsField(Common.MONGO_SHOP_VISITED_DWELL)) {
          isCustomer = dwellRet.head.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt > Common.CUSTOMER_JUDGE
        }

        val updateCol = MongoDBObject(Common.MONGO_SHOP_VISITED_TIMES -> times,
          Common.MONGO_SHOP_VISITED_IS_CUSTOMER -> isCustomer,
          Common.MONGO_SHOP_VISITED_PHONE_BRAND -> new String(visitor.getPhoneBrand.toByteArray))

        val update = MongoDBObject(Common.MONGO_OPTION_SET -> updateCol,
          Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_SHOP_VISITED_DWELL -> 1))

        visitedCollection.update(query, update, upsert = true)
        visitor.setIsCustomer(isCustomer)

        reList.append((record._1, visitor.build().toByteArray))
      })
    } catch {
      case e: Exception => println("ERROR calcDwellIsCustomer : " + e.printStackTrace())
    }

    reList.toIterator
  }

  /**
    * 已废弃 待删
    * @param rdd
    */
  def calcDwell(rdd: RDD[(String, Array[Byte])]): Unit = {

    rdd.foreachPartition {

      partition => {

        try {
          val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
          val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

          partition.foreach(record => {

            val visitor = Visitor.newBuilder().mergeFrom(record._2)
            val userMac = new String(visitor.getPhoneMac.toByteArray)
            val sceneId = visitor.getSceneId

            val historyQuery = MongoDBObject(Common.MONGO_SHOP_VISITED_SCENE_ID -> sceneId,
            Common.MONGO_SHOP_VISITED_MAC -> userMac)
            val historyFind = MongoDBObject(Common.MONGO_SHOP_VISITED_TIMES -> 1)

            val retList = historyCollection.find(historyQuery, historyFind).toList

            var times = 0
            if (retList.nonEmpty) {
              val ret = retList.head
              times = ret.get(Common.MONGO_SHOP_VISITED_TIMES).toString.toInt
            }

            val date = DateUtil.getDayTimestamp(visitor.getTimeStamp)

            //save
            val query = MongoDBObject(Common.MONGO_SHOP_VISITED_DATE -> date,
              Common.MONGO_SHOP_VISITED_SCENE_ID -> sceneId,
              Common.MONGO_SHOP_VISITED_MAC -> userMac)

            //db.shop_visited.ensureIndex({"date":1,"sceneId":1,"mac":1})
            val queryCol = MongoDBObject(Common.MONGO_SHOP_VISITED_DWELL -> 1,
              Common.MONGO_OPTION_ID -> 0)
            var isCustomer = false
            val dwellRet = visitedCollection.find(query, queryCol).toList
            if (dwellRet.nonEmpty && dwellRet.head.containsField(Common.MONGO_SHOP_VISITED_DWELL)) {
              isCustomer = dwellRet.head.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt > Common.CUSTOMER_JUDGE
            }

            val updateCol = MongoDBObject(Common.MONGO_SHOP_VISITED_TIMES -> times,
              Common.MONGO_SHOP_VISITED_IS_CUSTOMER -> isCustomer,
              Common.MONGO_SHOP_VISITED_PHONE_BRAND -> new String(visitor.getPhoneBrand.toByteArray))

            val update = MongoDBObject(Common.MONGO_OPTION_SET -> updateCol,
             Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_SHOP_VISITED_DWELL -> 1))

            visitedCollection.update(query, update, upsert = true)
          })
        } catch {
          case e: Exception => e.printStackTrace()
        }
      }
    }
  }

}
