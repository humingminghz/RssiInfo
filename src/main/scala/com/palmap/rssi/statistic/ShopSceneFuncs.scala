package com.palmap.rssi.statistic

import java.text.SimpleDateFormat

import com.mongodb.ServerAddress
import com.mongodb.casbah.MongoClient
import com.palmap.rssi.common.{GeneralMethods, Common}

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.io.Source

object ShopSceneFuncs {
  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def getApMacShopMap(): Map[String, Int] = {
    val mongoServerList = xmlConf(Common.MONGO_ADDRESS_LIST)
    val mongoServerArr = mongoServerList.split(",", -1)
    var serverList = ListBuffer[ServerAddress]()
    for (i <- 0 until mongoServerArr.length) {
      val server = new ServerAddress(mongoServerArr(i), xmlConf(Common.MONGO_SERVER_PORT).toInt)
      serverList.append(server)
    }
    val mongoClient = MongoClient(serverList.toList)
    val ApMacShopIdMap = scala.collection.mutable.Map[String, Int]()
    try {

      val db = mongoClient(xmlConf(Common.MONGO_DB_NAME))
      val collection = db(Common.MONGO_STORE_APMAC_RELATION)
      val mongoCusor = collection.find()
      if (mongoCusor != null) {
        for (mongoDoc <- mongoCusor) {
          ApMacShopIdMap(mongoDoc.get("apmac").toString().toLowerCase()) = mongoDoc.get("store_id").toString().toInt
        }
      }
    } finally {
      mongoClient.close()
    }

    ApMacShopIdMap.toMap
  }

  def getMacBrandMap(fileName: String): Map[String, String] = {
    var macBrandMap = scala.collection.mutable.Map[String, String]()
    Source.fromFile(fileName).getLines()
      .filter(_.split("\u0001").length == 2)
      .foreach { line =>
        val arr = line.split("\u0001")
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
    val mongoServerList = xmlConf(Common.MONGO_ADDRESS_LIST)
    val mongoServerArr = mongoServerList.split(",", -1)
    var serverList = ListBuffer[ServerAddress]()
    for (i <- 0 until mongoServerArr.length) {
      val server = new ServerAddress(mongoServerArr(i), xmlConf(Common.MONGO_SERVER_PORT).toInt)
      serverList.append(server)
    }
    val mongoClient = MongoClient(serverList.toList)
    val ApMacShopIdMap = scala.collection.mutable.Map[String, Int]()
    try {

      val db = mongoClient(xmlConf(Common.MONGO_DB_NAME))
      val collection = db(Common.MONGO_USER_TYPE_INFO)

      val typeCusor = collection.find()
      for (mongoDoc <- typeCusor) {
        val shopId = mongoDoc.get("planar_graph").toString().toInt //mongo.collection.userinfo.shopid
        val userType = mongoDoc.get("user_type").toString().toDouble.toInt.toString //mongo.collection.userinfo.type
        val userMac = mongoDoc.get("user_mac").toString().toLowerCase() //mongo.collection.userinfo.mac

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
      (machineMap.toMap, employeeMap.toMap)
    } finally {
      mongoClient.close()
    }
  }


}