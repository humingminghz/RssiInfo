package com.palmap.rssi.statistic

import java.text.SimpleDateFormat
import java.util.Date

import com.google.protobuf.ByteString
import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.FrostEvent.{IdType, RssiInfo, StubType}
import com.palmap.rssi.message.ShopStore.Visitor
import scala.collection.JavaConversions._

object ShopUnitFuncs {

  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)
  val apShopMap = ShopSceneFuncs.getApMacShopMap()
  val macBrandMap = ShopSceneFuncs.getMacBrandMap("mac_brand")
  val (machineMap, employeeMap) = ShopSceneFuncs.getMachineAndEmployeeMac()

//  def setUserType(partition: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {
//
//    val ret = scala.collection.mutable.Map[String, Array[Byte]]()
//
//    var currentDate = new Date()
//    currentDate = new Date(currentDate.getTime / 1000 * 1000)
//    currentDate.setHours(0)
//    currentDate.setMinutes(0)
//    currentDate.setSeconds(0)
//    val dateTime = currentDate.getTime
//    println("setUserType.................")
//    try {
//      val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)
//
//      partition.foreach(record => {
//        val visitor = Visitor.newBuilder().clear().mergeFrom(record._2, 0, record._2.length)
//        val phoneMac =  new String(visitor.getPhoneMac.toByteArray())
//        val locationId = visitor.getLocationId
//        val rssiList = visitor.getRssiList
//        val sceneId = visitor.getSceneId
//        var userType = Common.PASSENGER_VALUE
//        val machineMacSet = machineMap.getOrElse(locationId, null)
//        val employeeMacSet = employeeMap.getOrElse(locationId, null)
//
////        if (!machineMacSet.isEmpty && machineMacSet.contains(phoneMac)) {
////          println("machineMacSet:   " + Common.MACHINE_VALUE)
////          userType = Common.MACHINE_VALUE
////        } else if (!employeeMacSet.isEmpty && employeeMacSet.contains(phoneMac)) {
////          println("employeeMacSet:   " + Common.EMPLOYEE_VALUE)
////          userType = Common.EMPLOYEE_VALUE
////        } else if (visitor.getRssiCount <= 40) {
////          println("PASSENGER_VALUE:   " + Common.PASSENGER_VALUE)
////          userType = Common.PASSENGER_VALUE
////        } else {
////          println("CUSTOMER_VALUE:   " + Common.CUSTOMER_VALUE)
////          userType = Common.CUSTOMER_VALUE
////        }
//
//        val query = new BasicDBObject()
//        query.put(Common.MONGO_SHOP_VISITED_DATE, dateTime)
//        query.put(Common.MONGO_SHOP_VISITED_SCENEID, sceneId)
//        query.put(Common.MONGO_SHOP_VISITED_MAC, phoneMac)
//        val findObj = new BasicDBObject
//        findObj.put(Common.MONGO_SHOP_VISITED_USERTYPE, Common.PASSENGER_VALUE)
//
//        val retList = visitedCollection.find(query, findObj).toList
//        if (retList.size > 0) {
//          val ret = retList.head
//          var isCustomer = false;
//          isCustomer = (ret.get(Common.MONGO_SHOP_VISITED_USERTYPE).toString().toInt == Common.CUSTOMER_VALUE)
//
//          if (isCustomer) {
//            visitor.setUserType(Common.CUSTOMER_VALUE)
//          } else if (!isCustomer && userType == Common.CUSTOMER_VALUE) {
//
//            visitor.setUserType(userType)
//            val update = new BasicDBObject(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_SHOP_VISITED_USERTYPE, userType))
//            val ee = visitedCollection.update(query, update, true)
//          } else if (!isCustomer && userType != 0) {
//            visitor.setUserType(userType)
//          }
//
//        } else {
//
//          val update = new BasicDBObject(Common.MONGO_OPTION_SET, new BasicDBObject(Common.MONGO_SHOP_VISITED_USERTYPE, userType))
//          val re = visitedCollection.update(query, update, true)
//          visitor.setUserType(userType)
//        }
//        ret.put(record._1, visitor.build().toByteArray())
//      })
//
//    } catch {
//      case e: Exception => e.printStackTrace()
//    }
//    ret.iterator
//
//  }

  def fileterVisitor(event: Array[Byte]): Boolean = {
    var ret = false
    val frostEvent = RssiInfo.newBuilder()
    try {
      frostEvent.mergeFrom(event, 0, event.length)
      if (frostEvent.getStubType == StubType.AP && frostEvent.getIdType == IdType.MAC) {
        val apMac = frostEvent.getIdData.toLowerCase
        if (apShopMap.contains(apMac)) {
          ret = true
        }
      }

    } catch {
      case e: Exception => println("ERROR: " + e.getStackTrace)
    }

    ret
  }

  def visitorInfo(event: Array[Byte]): Map[String, Int] = {
    val frostEvent = RssiInfo.newBuilder()
    val shopRddMap = scala.collection.mutable.Map[String, Int]()
    try {
      frostEvent.mergeFrom(event, 0, event.length)
      if (frostEvent.getStubType == StubType.AP && frostEvent.getIdType == IdType.MAC && frostEvent.hasSceneId && frostEvent.hasTimestamp) {
        val timeStamp = frostEvent.getTimestamp
        val minuteFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
        val createDate = new Date(timeStamp)
        val createTime = minuteFormat.parse(minuteFormat.format(createDate)).getTime

        val currentDate = new Date()
        val currentTime = minuteFormat.parse(minuteFormat.format(currentDate)).getTime
        if (createTime == (currentTime - Common.BATCH_INTERVAL_IN_MILLI_SEC)) {
          val sceneId = frostEvent.getSceneId
          val apMac = frostEvent.getIdData.toLowerCase
          frostEvent.getItemsList.foreach(item => {
            val phoneMac = item.getIdData.toLowerCase
            val rssi = item.getRssi
            shopRddMap += (sceneId + "," + apMac + "," + phoneMac + "," + createTime -> rssi)
          })

        }
      }
    }
    catch {
      case e: Exception => e.getStackTrace
    }

    shopRddMap.toMap
  }

//  def visitorInfo(event: Array[Byte]): scala.collection.mutable.Map[String, Int] = {
//    val shopRddMap = scala.collection.mutable.Map[String, Int]()
//    val frostEvent = RssiInfo.newBuilder()
//    try {
//      frostEvent.mergeFrom(event, 0, event.length)
//      val apMac = frostEvent.getIdData
//      val sceneId = frostEvent.getSceneId
//      val timeStamp = frostEvent.getTimestamp
//      val rssiItems = frostEvent.getItemsList
//      for ( i <- 0 until rssiItems.size()) {
//        val rssiItem = rssiItems.get(i)
//        val phoneMac = rssiItem.getIdData
//        val rssi = rssiItem.getRssi
//        shopRddMap += (sceneId + "," + apMac + "," + phoneMac + "," + timeStamp -> rssi)
//      }
//    } catch {
//      case e: Exception => println("visitorInfo  ERROR: " + e.getStackTrace)
//    }
//
//    shopRddMap
//  }

//  def buildVisitor1(visitor: (String, Int)): (String, Array[Byte]) = {
//    val arr = visitor._1.split(",", -1)
//    val sceneId = arr(0).toInt
//    val apMac = arr(1)
//    val phoneMac = arr(2).toLowerCase
//    val timeStamp = arr(3).toLong // * 1000
//    val rssi = visitor._2.toInt
//    val locationId = apShopMap(apMac)
//    val visitorBuilder = Visitor.newBuilder()
//    visitorBuilder.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes))
//    val phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH)
//    var phoneBrand = Common.BRAND_UNKNOWN
//    if (macBrandMap.contains(phoneMacKey)) {
//      phoneBrand = macBrandMap(phoneMacKey)
//    }
//
//    visitorBuilder.setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes))
//    visitorBuilder.setSceneId(sceneId)
//    visitorBuilder.setLocationId(locationId)
//    visitorBuilder.addRssi(rssi)
//    visitorBuilder.addTimeStamp(timeStamp)
//
//    (phoneMac + Common.CTRL_A + locationId, visitorBuilder.build().toByteArray()) //+","+sceneId
//  }


  def buildVisitor(event: Iterator[(String, Int)]): Iterator[(String, Array[Byte])] = {

    val shopRddMap = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()
    event.foreach(visitor => {
      val arr = visitor._1.split(",", -1)
      val sceneId = arr(0).toInt
      val apMac = arr(1)
      val phoneMac = arr(2)
      val timeStamp = arr(3).toLong
      val rssi = visitor._2.toInt
      val locationId = apShopMap(apMac)

      val phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH)
      var phoneBrand = Common.BRAND_UNKNOWN
      if (macBrandMap.contains(phoneMacKey)) {
        phoneBrand = macBrandMap(phoneMacKey)
      }

      val visitorBuilder = Visitor.newBuilder()
      visitorBuilder.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes))
      visitorBuilder.setSceneId(sceneId)
      visitorBuilder.setTimeStamp(timeStamp)
      visitorBuilder.setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes))
      visitorBuilder.addRssi(rssi)

      shopRddMap.append((phoneMac + Common.CTRL_A + sceneId, visitorBuilder.build().toByteArray()))

    })

    shopRddMap.toIterator
  }

//  def buildVisitor(visitor: (String, Int)): (String, Array[Byte]) = {
//    val arr = visitor._1.split(",", -1)
//    val sceneId = arr(0).toInt
//    val apMac = arr(1)
//    val phoneMac = arr(2).toLowerCase
//    val timeStamp = arr(3).toLong // * 1000
//    val rssi = visitor._2.toInt
//    val locationId = apShopMap(apMac)
//    val visitorBuilder = Visitor.newBuilder()
//    visitorBuilder.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes))
//    val phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH)
//    var phoneBrand = Common.BRAND_UNKNOWN
//    if (macBrandMap.contains(phoneMacKey)) {
//      phoneBrand = macBrandMap(phoneMacKey)
//    }
//
//    visitorBuilder.setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes))
//    visitorBuilder.setSceneId(sceneId)
//    visitorBuilder.setLocationId(locationId)
//    visitorBuilder.addRssi(rssi)
//    visitorBuilder.addTimeStamp(timeStamp)
//
//    (phoneMac + Common.CTRL_A + locationId, visitorBuilder.build().toByteArray()) //+","+sceneId
//  }


//  def isNumeric(str: String): Boolean = {
//    val pattern1 = Pattern.compile("[-][0-9]*")
//    val pattern2 = Pattern.compile("[0-9]*")
//    (pattern1.matcher(str).matches() || pattern2.matcher(str).matches())
//  }


}