package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.{WriteConcern, BasicDBObject}
import com.palmap.rssi.common.{Common, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ListBuffer

/**
 * Created by admin on 2015/12/22.
 */
object VisitedFuncs {

  def calVisitorDwell(iter: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {
    val retList = ListBuffer[(String, Array[Byte])]()
    val visitorCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
    val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

    iter.foreach(item => {
      val visitorBuilder = Visitor.newBuilder().mergeFrom(item._2, 0, item._2.length)
      val sceneId = visitorBuilder.getSceneId
      val mac = new String(visitorBuilder.getPhoneMac.toByteArray)
      val timeStamp = visitorBuilder.getTimeStamp
      val phoneBrand = new String(visitorBuilder.getPhoneBrand.toByteArray)

      val queryBasic = new BasicDBObject()
        .append(Common.MONGO_HISTORY_SHOP_SCENEID, sceneId)
        .append(Common.MONGO_HISTORY_SHOP_MAC, mac)

      val findBasic = new BasicDBObject()
        .append(Common.MONGO_OPTION_ID, 0)
        .append(Common.MONGO_HISTORY_SHOP_TIMES, 1)

      var times = 0
      val reList = historyCollection.find(queryBasic, findBasic).toList
      if (reList.size > 0) {
        times = reList.head.get(Common.MONGO_HISTORY_SHOP_TIMES).toString.toInt
      }

      val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
      val dayTime = sdf.parse(sdf.format(new Date(timeStamp))).getTime

      val queryVisit = new BasicDBObject()
        .append(Common.MONGO_SHOP_VISITED_DATE, dayTime)
        .append(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
        .append(Common.MONGO_SHOP_VISITED_MAC, mac)

      val findDwell = new BasicDBObject().append(Common.MONGO_SHOP_VISITED_DWELL, 1).append(Common.MONGO_OPTION_ID, 0)

      val reDwell = visitorCollection.find(queryVisit, findDwell).toList
      var isCustomer = false
      if (reDwell.size > 0 && reDwell.head.containsField(Common.MONGO_SHOP_VISITED_DWELL)) {
        isCustomer = reDwell.head.get(Common.MONGO_SHOP_VISITED_DWELL).toString.toInt > Common.CUSTOMER_JUDGE
      }

      val updateBasic = new BasicDBObject()
        .append(Common.MONGO_SHOP_VISITED_TIMES, times)
        .append(Common.MONGO_SHOP_VISITED_ISCUSTOMER, isCustomer)
        .append(Common.MONGO_SHOP_VISITED_PHONEBRAND, phoneBrand)

      val updateCol = new BasicDBObject()
        .append(Common.MONGO_OPTION_SET, updateBasic)
        .append(Common.MONGO_OPTION_INC, new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, 1))

      visitorCollection.update(queryVisit, updateCol, true)

      visitorBuilder.setIsCustomer(isCustomer)
      retList += ((item._1, visitorBuilder.build().toByteArray))
    })

    retList.toIterator
  }

  def calcDwellIsCustomer(iter: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {
    val reList = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()

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

    reList.toIterator
  }


  def calcDwell(rdd: RDD[(String, Array[Byte])]): Unit = {
    rdd.foreachPartition { partition =>
    {
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
