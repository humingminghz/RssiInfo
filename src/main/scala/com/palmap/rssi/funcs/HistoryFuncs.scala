package com.palmap.rssi.funcs

import com.mongodb.casbah.commons.MongoDBObject
import com.palmap.rssi.common.{Common, DateUtil, MongoFactory}
import com.palmap.rssi.message.ShopStore.Visitor
import org.apache.spark.rdd.RDD
import org.json.JSONArray

/**
  * 将计算出的数据更新至history表中
  */
object HistoryFuncs {

  /**
    * 保存history表信息
    * @param rdd 待处理rdd
    */
  def saveHistory(rdd: RDD[(String, Array[Byte])]): Unit = {

    try {
      rdd.foreachPartition(partition => { // 遍历所有partition

        val historyCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_HISTORY)

        partition.foreach(record => {

          val visitorBuilder = Visitor.newBuilder().mergeFrom(record._2, 0, record._2.length)
          val sceneId = visitorBuilder.getSceneId
          val mac = new String(visitorBuilder.getPhoneMac.toByteArray)
          val minuteTime = visitorBuilder.getTimeStamp
          var dayTime = DateUtil.getDayTimestamp(minuteTime)

          // 从history表中查询当前mac的历史信息
          val queryBasic = MongoDBObject(Common.MONGO_HISTORY_SHOP_SCENE_ID -> sceneId,
            Common.MONGO_HISTORY_SHOP_MAC -> mac)
          val findBasic = MongoDBObject(Common.MONGO_HISTORY_SHOP_DAYS -> MongoDBObject(Common.MONGO_OPTION_SLICE -> List[Int](-1, 1)),
            Common.MONGO_HISTORY_SHOP_FIRST_DATE -> 1)
          // 更新最后来的时间 以及所有来过的天数列表
          val updateBasic = MongoDBObject(Common.MONGO_OPTION_INC -> MongoDBObject(Common.MONGO_HISTORY_SHOP_TIMES -> 1),
            Common.MONGO_OPTION_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_LAST_DATE -> dayTime),
            Common.MONGO_OPTION_ADD_TO_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_DAYS -> dayTime))

          val retList = historyCollection.findOne(queryBasic, findBasic).toList
          var isDateExist = false

          // 对已有的history中的数据更新
          if (retList.nonEmpty) {
            val record = retList.head
            if (! record.containsField(Common.MONGO_HISTORY_SHOP_FIRST_DATE)) { // 是否有来店第一天的数据
              val firstDayCol = MongoDBObject(Common.MONGO_HISTORY_SHOP_DAYS -> MongoDBObject(Common.MONGO_OPTION_SLICE -> 1))
              val firstDay = historyCollection.find(queryBasic, firstDayCol).toList
              if (firstDay.nonEmpty) {
                dayTime = new JSONArray(firstDay.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString).getLong(0)
                if (sceneId == 10062) println("sceneId: " + sceneId + "    firstTime: " + dayTime  +"  mac : "+ mac)
              }
              // 更新第一次来店时间
              val firstUpdate = MongoDBObject(Common.MONGO_OPTION_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_FIRST_DATE -> dayTime))
              historyCollection.update(queryBasic, firstUpdate, upsert = true)
            }

            val latestDate = new JSONArray(retList.head.get(Common.MONGO_HISTORY_SHOP_DAYS).toString).getLong(0)
            isDateExist = latestDate >= dayTime
          }

          if (!isDateExist) {
            if (retList.isEmpty) {
              val firstUpdate = MongoDBObject(Common.MONGO_OPTION_SET -> MongoDBObject(Common.MONGO_HISTORY_SHOP_FIRST_DATE -> dayTime))
              historyCollection.update(queryBasic, firstUpdate, upsert = true)
            }

            historyCollection.update(queryBasic, updateBasic, upsert = true)
          }
        })
      })
    } catch {
      case e: Exception => println( "ERROR  saveHistory: " + e.printStackTrace())
    }
  }

}
