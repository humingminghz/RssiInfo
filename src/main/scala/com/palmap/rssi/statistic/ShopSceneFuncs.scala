package com.palmap.rssi.statistic

import java.text.SimpleDateFormat

import com.palmap.rssi.common.{MongoFactory, GeneralMethods, Common}

import scala.collection.JavaConversions._
import scala.io.Source

object ShopSceneFuncs {
  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def getApMacShopMap(): Map[String, Int] = {

    val ApMacShopIdMap = scala.collection.mutable.Map[String, Int]()
    try {
      val apMacCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_STORE_APMAC)
      val mongoCusor = apMacCollection.find()
      if (!mongoCusor.isEmpty) {
        for (mongoDoc <- mongoCusor) {
          ApMacShopIdMap += (mongoDoc.get(Common.MONGO_STORE_APMAC_APMAC).toString().toLowerCase() -> mongoDoc.get(Common.MONGO_STORE_APMAC_SCENEID).toString().toDouble.toInt)
        }
      }

    } catch {
      case e: Exception => e.printStackTrace()
    }

    ApMacShopIdMap.toMap
  }

  def getMacBrandMap(fileName: String): Map[String, String] = {
    var macBrandMap = scala.collection.mutable.Map[String, String]()
    Source.fromFile(fileName).getLines()
      .filter(_.split(Common.CTRL_A).length == 2)
      .foreach { line =>
        val arr = line.split(Common.CTRL_A)
        macBrandMap(arr(0)) = arr(1)
      }
    macBrandMap.toMap
  }

  def getRssiAvg(timeStampList: java.util.List[Integer]): Int = {
    if (timeStampList.size() == 0) {
      -100
    } else {
      var sum = 0
      timeStampList.foreach { i =>
        sum += i
      }

      sum / timeStampList.size()
    }
  }


  def getMachineAndEmployeeMac(): (Map[Int, scala.collection.mutable.Set[String]], Map[Int, scala.collection.mutable.Set[String]]) = {

    val machineMap = scala.collection.mutable.Map[Int, scala.collection.mutable.Set[String]]()
    val employeeMap = scala.collection.mutable.Map[Int, scala.collection.mutable.Set[String]]()
    val ApMacShopIdMap = scala.collection.mutable.Map[String, Int]()
    try {
      val userTypeCollection = MongoFactory.getDBCollection(Common.MONGO_USER_TYPE_INFO)
      val typeCusor = userTypeCollection.find()
      for (mongoDoc <- typeCusor) {
        val shopId = mongoDoc.get("shopId").toString().toInt //mongo.collection.userinfo.shopid
        val userType = mongoDoc.get("userType").toString() //mongo.collection.userinfo.type
        val userMac = mongoDoc.get("userMac").toString().toLowerCase() //mongo.collection.userinfo.mac

        userType match {
          case "0" => {
            if (machineMap.contains(shopId)) {
              machineMap(shopId).add(userMac)
            } else {
              val macSet = scala.collection.mutable.Set[String]()
              macSet.add(userMac)
              machineMap.put(shopId, macSet)
            }
          }
          case "1" => {
            if (employeeMap.contains(shopId)) {
              employeeMap(shopId).add(userMac)
            } else {
              val macSet = scala.collection.mutable.Set[String]()
              macSet.add(userMac)
              employeeMap.put(shopId, macSet)
            }
          }
        }
      }

    } catch {
      case e: Exception => e.printStackTrace()
    }

    (machineMap.toMap, employeeMap.toMap)
  }


}