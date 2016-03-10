package com.palmap.rssi.common

import java.text.SimpleDateFormat

import com.mongodb.{BasicDBObject, ServerAddress}
import com.mongodb.casbah.MongoClient

import scala.collection.mutable.{HashMap, ListBuffer}
import scala.io.Source

/**
 * Created by lingling.dai on 2016/1/12.
 */
object ConfInfoSet {

  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def getBusinessHoursMap:Unit={
    val businessHoursColl = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_STATICINFO)
    val businessHoursList = businessHoursColl.find().toList
    if (!businessHoursList.isEmpty)
      businessHoursList.foreach(mongoDoc => {
        val sceneId = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_SCENEID).toString.toInt
        val openMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_OPENMINUTE).toString.toInt
        val closeMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_CLOSEMINUTE).toString.toInt
        CommonConf.businessHoursMap += (sceneId ->(openMinute, closeMinute))
      })

  }

  def updateBusinessHourMap(sceneId:Int):Unit={
    val staticInfoColl = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_STATICINFO)
    val query = new BasicDBObject()
    query.put(Common.MONGO_STATICINFO_SHOP_SCENEID, sceneId)
    val staticInfoList = staticInfoColl.find(query)
    if (!staticInfoList.isEmpty)
      staticInfoList.foreach(mongoDoc => {
        val sceneId = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_SCENEID).toString.toInt
        val openMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_OPENMINUTE).toString.toInt
        val closeMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_CLOSEMINUTE).toString.toInt
        CommonConf.businessHoursMap +=(sceneId->(openMinute, closeMinute))
      })
  }


}
