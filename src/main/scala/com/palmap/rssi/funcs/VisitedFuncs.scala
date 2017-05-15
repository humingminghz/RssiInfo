package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.{BasicDBObject, WriteConcern}
import com.palmap.rssi.common.{Common, CommonConf, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
  * Created by admin on 2015/12/22.
  */
object VisitedFuncs {

  def calVisitorDwell(iter: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {
    val retList = ListBuffer[(String, Array[Byte])]()
    try {
      val visitorCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
      val realTimeCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_REALTIME)
      //      val shopTypeInfoCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_TYPE_INFO)

      iter.foreach(item => {
        val visitorBuilder = Visitor.newBuilder().mergeFrom(item._2, 0, item._2.length)
        val sceneId = visitorBuilder.getSceneId
        val mac = new String(visitorBuilder.getPhoneMac.toByteArray).toUpperCase
        val timeStamp = visitorBuilder.getTimeStamp
        val phoneBrand = new String(visitorBuilder.getPhoneBrand.toByteArray)


        val queryBasic = new BasicDBObject()
          .append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
          .append(Common.MONGO_HISTORY_SHOP_MAC, mac.toLowerCase)

        val findBasic = new BasicDBObject()
          .append(Common.MONGO_OPTION_ID, 0)
          .append(Common.MONGO_HISTORY_SHOP_FIRSTDATE, 1)
          .append(Common.MONGO_HISTORY_SHOP_LASTDATE, 1)
          .append(Common.MONGO_HISTORY_SHOP_TIMES, 1)

        //历史查询,获取到访次数
        val reList = historyCollection.find(queryBasic, findBasic).toList
        var times = 0
        var freq: Int = 0
        if (reList.size > 0) {
          val record = reList.head
          times = record.get(Common.MONGO_HISTORY_SHOP_TIMES).toString.toInt
          val firstDate = record.get(Common.MONGO_HISTORY_SHOP_FIRSTDATE).toString.toLong
          val lastDate = record.get(Common.MONGO_HISTORY_SHOP_LASTDATE).toString.toLong
          if (times > 1) freq = ((lastDate - firstDate) / Common.DAY_FORMATER).toInt / (times - 1)
        }

        val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
        val dayTime = sdf.parse(sdf.format(new Date(timeStamp))).getTime

        val realQuery = new BasicDBObject()
          .append(Common.MONGO_SHOP_REALTIME_SCENEID, sceneId)
          .append(Common.MONGO_SHOP_REALTIME_MACS, mac)
          .append(Common.MONGO_SHOP_REALTIME_TIME, new BasicDBObject(Common.MONGO_OPTION_GTE, timeStamp - Common.HOUR_FORMATER))
        val realfind = new BasicDBObject().append(Common.MONGO_OPTION_ID, 0).append(Common.MONGO_SHOP_REALTIME_TIME, 1)
        //查询前一次的时间
        val minuteList = realTimeCollection.find(realQuery, realfind).sort(new BasicDBObject(Common.MONGO_SHOP_REALTIME_TIME, -1)).limit(1).toList
        var minuteAgo = 0L
        if (minuteList.size > 0) {
          minuteAgo = minuteList.head.get(Common.MONGO_SHOP_REALTIME_TIME).toString.toLong
        }
        //间隔时间(分钟)
        val intervalTime = ((timeStamp - minuteAgo) / Common.MINUTE_FORMATER).toInt
        var dwell = 1
        //判定小于等于10分钟的，dwell进行累计； 否则，判定为2次进店或路过，dwell为1
        if (intervalTime <= Common.INTERVATE_MINUTE) {
          dwell = intervalTime;
        }
        //        val queryTypeInfo = new BasicDBObject(Common.MONGO_COLLECTION_SHOP_SCENEIDS, sceneId)
        //        val colTypeInfo = new BasicDBObject(Common.MONGO_OPTION_ID, 0).append(Common.MONGO_COLLECTION_SHOP_CUSTOMERDWELL, 1)
        //查询顾客判定条件
        //        val dwellList = shopTypeInfoCollection.find(queryTypeInfo, colTypeInfo).toList
        //        var customerFlag = 0
        //        if (dwellList.size > 0) {
        //          customerFlag = dwellList.head.get(Common.MONGO_COLLECTION_SHOP_CUSTOMERDWELL).toString.toInt
        //        }

        val queryVisit = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_DATE, dayTime)
          .append(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
          .append(Common.MONGO_SHOP_VISITED_MAC, mac)

        val findDwell = new BasicDBObject().append(Common.MONGO_SHOP_VISITED_DWELL, 1).append(Common.MONGO_OPTION_ID, 0)

        val reDwell = visitorCollection.find(queryVisit, findDwell).toList
        var isCustomer = false
        if (reDwell.size > 0 && reDwell.head.containsField(Common.MONGO_SHOP_VISITED_DWELL)) {
          val dwellAgo = reDwell.head.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt
          //add by yuyingchao
          if (CommonConf.sceneIdMap.contains(sceneId)) {
            val setDwell = CommonConf.sceneIdMap.getOrElse(sceneId, 5)
            isCustomer = dwellAgo + dwell > setDwell
          } else if (sceneId == 10062) {
            isCustomer = !(phoneBrand.equals(Common.BRAND_UNKNOWN));
          } else {
            isCustomer = dwellAgo + dwell > 5
          }
        }

        if (CommonConf.sceneIdMap.get(sceneId) == Some(0)) {
          val setDwell = CommonConf.sceneIdMap.getOrElse(sceneId, 5)

          isCustomer = dwell > setDwell
        }

        val updateBasic = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_TIMES, times)
          .append(Common.MONGO_SHOP_VISITED_FREQUENCY, freq)
          .append(Common.MONGO_SHOP_VISITED_ISCUSTOMER, isCustomer)
          .append(Common.MONGO_SHOP_VISITED_PHONEBRAND, phoneBrand)

        val updateCol = new BasicDBObject()
          .append(Common.MONGO_OPTION_SET, updateBasic)
          .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, dwell))

        visitorCollection.update(queryVisit, updateCol, true)

        visitorBuilder.setIsCustomer(isCustomer)
        retList += ((item._1, visitorBuilder.build().toByteArray))
      })
    }
    catch {
      case e: Exception => println("ERROR calVisitorDwell: " + e.printStackTrace())
    }
    retList.toIterator

  }

  def calcDwellIsCustomer(iter: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {
    val reList = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()

    try {
      val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
      val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

      iter.foreach(record => {
        val visitor = Visitor.newBuilder().mergeFrom(record._2)
        val userMac = new String(visitor.getPhoneMac.toByteArray())
        val sceneId = visitor.getSceneId

        val historyQuery = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
          .append(Common.MONGO_SHOP_VISITED_MAC, userMac)

        val historyFind = new BasicDBObject(Common.MONGO_SHOP_VISITED_TIMES, 1)

        var times = 0;
        val retList = historyCollection.find(historyQuery, historyFind).toList
        if (retList.size > 0) {
          val ret = retList.head
          times = ret.get(Common.MONGO_SHOP_VISITED_TIMES).toString.toInt
        }

        val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
        val todayDate = todayDateFormat.format(new Date(visitor.getTimeStamp))
        val date = todayDateFormat.parse(todayDate).getTime

        //save
        val query = new BasicDBObject
        query.append(Common.MONGO_SHOP_VISITED_DATE, date)
        query.append(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
        query.append(Common.MONGO_SHOP_VISITED_MAC, userMac)

        //db.shop_visited.ensureIndex({"date":1,"sceneId":1,"mac":1})
        val queryCol = new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, 1).append(Common.MONGO_OPTION_ID, 0)

        var isCustomer = false
        val dwellRet = visitedCollection.find(query, queryCol).toList
        if (dwellRet.size > 0 && dwellRet.head.containsField(Common.MONGO_SHOP_VISITED_DWELL)) {
          isCustomer = dwellRet.head.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt > Common.CUSTOMER_JUDGE
        }

        val updateCol = new BasicDBObject()
          .append(Common.MONGO_SHOP_VISITED_TIMES, times)
          .append(Common.MONGO_SHOP_VISITED_ISCUSTOMER, isCustomer)
          .append(Common.MONGO_SHOP_VISITED_PHONEBRAND, new String(visitor.getPhoneBrand.toByteArray()))

        val update = new BasicDBObject()
          .append(Common.MONGO_OPTION_SET, updateCol)
          .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, 1))

        visitedCollection.update(query, update, true)
        visitor.setIsCustomer(isCustomer)

        reList.append((record._1, visitor.build().toByteArray))
      })
    }
    catch {
      case e: Exception => println("ERROR calcDwellIsCustomer : " + e.printStackTrace())
    }

    reList.toIterator
  }


  def calcDwell(rdd: RDD[(String, Array[Byte])]): Unit = {
    rdd.foreachPartition { partition => {
      try {
        val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)
        partition.foreach(record => {

          val visitor = Visitor.newBuilder().mergeFrom(record._2)
          val userMac = new String(visitor.getPhoneMac.toByteArray())
          val sceneId = visitor.getSceneId

          val historyQuery = new BasicDBObject()
            .append(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
            .append(Common.MONGO_SHOP_VISITED_MAC, userMac)

          val historyFind = new BasicDBObject(Common.MONGO_SHOP_VISITED_TIMES, 1)

          var times = 0;
          val retList = historyCollection.find(historyQuery, historyFind).toList
          if (retList.size > 0) {
            val ret = retList.head
            times = ret.get(Common.MONGO_SHOP_VISITED_TIMES).toString.toInt
          }

          val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          val todayDate = todayDateFormat.format(new Date(visitor.getTimeStamp))
          val date = todayDateFormat.parse(todayDate).getTime

          //save
          val query = new BasicDBObject
          query.append(Common.MONGO_SHOP_VISITED_DATE, date)
          query.append(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
          query.append(Common.MONGO_SHOP_VISITED_MAC, userMac)

          //db.shop_visited.ensureIndex({"date":1,"sceneId":1,"mac":1})
          val queryCol = new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, 1).append(Common.MONGO_OPTION_ID, 0)

          var isCustomer = false
          val dwellRet = visitedCollection.find(query, queryCol).toList
          if (dwellRet.size > 0 && dwellRet.head.containsField(Common.MONGO_SHOP_VISITED_DWELL)) {
            isCustomer = dwellRet.head.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt > Common.CUSTOMER_JUDGE
          }

          val updateCol = new BasicDBObject()
            .append(Common.MONGO_SHOP_VISITED_TIMES, times)
            .append(Common.MONGO_SHOP_VISITED_ISCUSTOMER, isCustomer)
            .append(Common.MONGO_SHOP_VISITED_PHONEBRAND, new String(visitor.getPhoneBrand.toByteArray()))

          val update = new BasicDBObject()
            .append(Common.MONGO_OPTION_SET, updateCol)
            .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, 1))

          visitedCollection.update(query, update, true)
        })
      } catch {
        case e: Exception => e.printStackTrace()
      }

    }
    }
  }

}
