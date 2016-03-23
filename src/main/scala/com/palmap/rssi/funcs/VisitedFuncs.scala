package com.palmap.rssi.funcs

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.{WriteConcern, BasicDBObject}
import com.palmap.rssi.common.{Common, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD

/**
 * Created by admin on 2015/12/22.
 */
object VisitedFuncs {

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
          query.put(Common.MONGO_SHOP_VISITED_DATE, date)
          query.put(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
          query.put(Common.MONGO_SHOP_VISITED_MAC, userMac)

          //db.shop_visited.ensureIndex({"date":1,"sceneId":1,"mac":1})
          val queryCol = new BasicDBObject(Common.MONGO_SHOP_VISITED_DWELL, 1).append(Common.MONGO_OPTION_ID, 0)

          var isCustomer = false;
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
