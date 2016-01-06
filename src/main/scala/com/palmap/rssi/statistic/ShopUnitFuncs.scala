package com.palmap.rssi.statistic

import java.io.StringReader
import java.util.Date
import java.util.regex.Pattern
import javax.json.Json

import com.mongodb.casbah.MongoClient
import com.mongodb.{BasicDBObject, ServerAddress}
import com.palmap.rssi.common.{Common, GeneralMethods}
import com.palmap.rssi.message.ShopStore.Visitor
import scala.collection.mutable.ListBuffer

object ShopUnitFuncs {

  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)
  def setUserType(partition: Iterator[(String, Array[Byte])], machineMap: Map[Int, scala.collection.mutable.Set[String]], employeeMap: Map[Int, scala.collection.mutable.Set[String]]): Iterator[(String, Array[Byte])] = {

    val ret = scala.collection.mutable.Map[String, Array[Byte]]()

    var currentDate = new Date()
    val currentSec = currentDate.getTime / 1000 * 1000
    currentDate = new Date(currentSec - 30 * 1000)
    currentDate.setHours(0)
    currentDate.setMinutes(0)
    currentDate.setSeconds(0)
    val dateTime = currentDate.getTime

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
      partition.foreach(record => {
        val visitor = Visitor.newBuilder().clear().mergeFrom(record._2, 0, record._2.length)
        val phoneMac =  new String(visitor.getPhoneMac.toByteArray())
        val locationId = visitor.getLocationId
        val rssiList = visitor.getRssiList
        var userType = 1
        val machineMacSet = machineMap.getOrElse(locationId, null)
        val employeeMacSet = employeeMap.getOrElse(locationId, null)
        if (machineMacSet != null &&
          machineMacSet.contains(phoneMac)) {
          userType = Common.MACHINE_VALUE
        } else if (employeeMacSet != null &&
          employeeMacSet.contains(phoneMac)) {
          userType = Common.EMPLOYEE_VALUE
        } else if (visitor.getRssiCount <= 40) {
          userType =Common.PASSENGER_VALUE
        } else {
          userType = 0
        }

        val query = new BasicDBObject()
        query.put(Common.MONGO_VISITED_DATE, dateTime)
        query.put(Common.MONGO_VISITED_LOCATIONID, locationId)
        query.put(Common.MONGO_VISITED_SCENEID, visitor.getSceneId)
        query.put(Common.MONGO_VISITED_MAC, new String(visitor.getPhoneMac.toByteArray()))
        val findObj = new BasicDBObject
        findObj.put(Common.MONGO_VISITED_USERTYPE, 1)
        visitedCollection.find(query)

        val retList = visitedCollection.find(query, findObj).toList
        if (retList.size > 0) {
          val ret = retList.head
          var isCustomer = false;
          val typeUser = ret.get(Common.MONGO_VISITED_USERTYPE).toString().toInt
          isCustomer = (typeUser== Common.CUSTOMER_VALUE)
          if (isCustomer) {
            visitor.setUserType(Common.CUSTOMER_VALUE)
          } else if (!isCustomer && userType == 0) {
            isCustomer = true
            val update = new BasicDBObject
            update.put(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_VISITED_USERTYPE, userType))
            visitedCollection.update(query, update, true)
          } else if (!isCustomer && userType != 0) {
            visitor.setUserType(userType)
          }
        } else {
          val update = new BasicDBObject
          update.put(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_VISITED_USERTYPE, userType))
          visitedCollection.update(query, update, true)
          visitor.setUserType(userType)
        }
        ret.put(record._1, visitor.build().toByteArray())
      })
    } finally {
      mongoClient.close()
    }
    ret.iterator

  }
  def fileterVisitor(x: String, apShopMap: Map[String, Int]): Boolean = {
    var ret = false
    try {
      val sr = new StringReader(x)
      val jsonObject = Json.createReader(sr).readObject()
      val apMac = jsonObject.getString("apMac").toLowerCase()
      if (!apShopMap.contains(apMac)) {
        ret = false
      } else {
        val sceneId = jsonObject.getJsonNumber("sceneId").toString()
        val timeStamp = jsonObject.getJsonNumber("timestamp").toString()
        val rssiArray = jsonObject.getJsonArray("rssiArray")
        for (i <- 0 until rssiArray.size()) {
          val rssiobj = rssiArray.getJsonObject(i)

          val phoneMac = rssiobj.getString("clientMac")
          val rssi = rssiobj.getJsonNumber("rssi").toString()

          if (isNumeric(timeStamp) || isNumeric(rssi) || isNumeric(sceneId)) {
            ret = false
          } else {
            val todayFormat = ShopSceneFuncs.todayFormat
            val currentDate = new Date()
            val currentTs = currentDate.getTime
            val todayDate = todayFormat.format(currentDate)
            val todayTs = todayFormat.parse(todayDate).getTime
            val nextDayTs = todayTs + 30 * 1000
            if (currentTs > nextDayTs && currentTs < nextDayTs + 30 * 1000) {
              ret = (timeStamp.toLong) >= todayTs
            } else {
              ret = true
            }
          }
        }
      }
    } catch {
      case ex: Exception => ret = false
    }
    ret
  }

  def isNumeric(str: String): Boolean = {
    val pattern1 = Pattern.compile("[-][0-9]*")
    val pattern2 = Pattern.compile("[0-9]*")
    !(pattern1.matcher(str).matches() || pattern2.matcher(str).matches())
  }


}