package com.palmap.rssi.statistic

import java.text.SimpleDateFormat
import java.util.Date

import com.google.protobuf.ByteString
import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{CommonConf, Common, GeneralMethods, MongoFactory}
import com.palmap.rssi.message.FrostEvent.{IdType, RssiInfo, StubType}
import com.palmap.rssi.message.ShopStore.Visitor
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.{ListBuffer, ArrayBuffer}

object ShopUnitFuncs {

  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)
  val macBrandMap: Map[String, String] = CommonConf.macBrandMap
  val businessHoursMap: mutable.Map[Int, (Int, Int)] = CommonConf.businessHoursMap
  val sceneIdList: mutable.Set[Int] = CommonConf.sceneIdlist

  /**
    * 从kafka传入的rssiInfo 提取相应字段 为构建visitor准备
    *
    * @param rssiInfo
    * @return (sceneId  + Common.CTRL_A + userMac + Common.CTRL_A + minuteTime -> (rssiList, appList, isConnected))
    */
  def rssiInfo(rssiInfo: Array[Byte]): mutable.HashMap[String, (ArrayBuffer[Int],ArrayBuffer[Int], Boolean) ]= {

    val rssiInfoMap = mutable.HashMap[String,(ArrayBuffer[Int],ArrayBuffer[Int], Boolean)]()
    val builder = RssiInfo.newBuilder().mergeFrom(rssiInfo, 0, rssiInfo.length)
    val timeStamp = builder.getTimestamp

    if (builder.hasSceneId && builder.hasTimestamp && builder.hasIdData
      && builder.getIdType == IdType.MAC && builder.getStubType == StubType.AP ) { // 必要字段检查

      val sceneId = builder.getSceneId
      val sdf = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
      val dateStr = sdf.format(new Date(timeStamp))
      val minuteTime = sdf.parse(dateStr).getTime
      val currentDate = new Date()
      val currentTime = sdf.parse(sdf.format(currentDate)).getTime

      //过滤本批次要处理的数据
      if (minuteTime == (currentTime - Common.MINUTE_FORMATTER)) {
        builder.getItemsList.foreach(item => {

          val userMac = item.getIdData.toUpperCase()
          if (!CommonConf.machineBrandSet.contains(userMac.substring(0, 8)) &&
                  item.hasIdData && item.hasRssi && item.getIdType == IdType.MAC) { // mac不在黑名单内 rssi相关数据存在

            val appList = ArrayBuffer[Int](1)                   // to judge how many aps contain mac
            val rssi = item.getRssi
            // 华为场景 添加 Connected 字段 其它场景 默认false 后面不处理其他场景数据
            var isConnected = false
            if(sceneId == Common.SCENE_ID_HUAWEI){
              isConnected = item.getConnected
            }

            val key = sceneId + Common.CTRL_A + userMac + Common.CTRL_A + minuteTime
            var rssiList = ArrayBuffer[Int]()
            // 已存在的key需要将value拿出来后追加当前rssi
            if (rssiInfoMap.contains(key)) {
              rssiList = rssiInfoMap(key)._1
              // 当前key的isConnected false, 但是有一次是true了
              if(isConnected == false && rssiInfoMap(key)._3 == true){
                isConnected = true
              }
            }

            rssiList += rssi
            rssiInfoMap += (sceneId  + Common.CTRL_A + userMac + Common.CTRL_A + minuteTime -> (rssiList, appList, isConnected))
          }
        })
      }
    }
//    println("rssiInfoMap: " + rssiInfoMap.size)
    rssiInfoMap
  }

  /**
    * 营业时间过滤
    *
    * @return
    */
  def filterBusinessVisitor(partition: Iterator[(String, (ArrayBuffer[Int],ArrayBuffer[Int], Boolean))]):Iterator[(String, (ArrayBuffer[Int],ArrayBuffer[Int], Boolean))] = {

    val recordList = ListBuffer[(String, ( ArrayBuffer[Int],ArrayBuffer[Int], Boolean))]()

    partition.foreach(record => {

      val info = record._1.split(Common.CTRL_A, -1)
      val sceneId = info(0).toInt
      val minuteTime = info(2).toLong
      val mac = info(1)

      if (CommonConf.sceneIdlist.contains(sceneId)) {

        var isFlag = false

        if (CommonConf.businessHoursMap.contains(sceneId)) {
          val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
          val date = todayDateFormat.parse(todayDateFormat.format(minuteTime)).getTime
          val openMinute = date + CommonConf.businessHoursMap(sceneId)._1 * Common.MINUTE_FORMATTER
          val closeMinute = (date + CommonConf.businessHoursMap(sceneId)._2 * Common.MINUTE_FORMATTER) - 1
          isFlag = minuteTime >= openMinute && minuteTime <= closeMinute
//          println("isFlag: " + isFlag)
        } else {
//          println("CommonConf.businessHoursMap.contains(sceneId) == false")
          isFlag = true
        }

        if(CommonConf.machineSet.contains(mac.toLowerCase())) {
//          println("CommonConf.machineSet.contains(mac.toLowerCase()) == true")
          isFlag = false
        }

        if (isFlag) {
          recordList += record
        }
      }
    })

//    println("recordList: " + recordList.size)
    recordList.toIterator
  }

  def buildVisitor(iterator: Iterator[(String, (ArrayBuffer[Int],ArrayBuffer[Int], Boolean))]): Iterator[(String, Array[Byte])] = {

    val reList = ListBuffer[(String, Array[Byte])]()

    iterator.foreach(event => {

        val keyInfo = event._1.split(Common.CTRL_A, -1)
        val sceneId = keyInfo(0).toInt
        val phoneMac = keyInfo(1).toLowerCase
        val minuteTime = keyInfo(2).toLong
        val rssiList = event._2._1.sorted

        val phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH)
        var phoneBrand = Common.BRAND_UNKNOWN

        if (macBrandMap.contains(phoneMacKey)) {
          phoneBrand = macBrandMap(phoneMacKey)
        }

        if( sceneId == 11340 && event._2._2.length == 2){         // ap1 and ap2 contain mac

          val visitor = Visitor.newBuilder()
             .setSceneId(sceneId)
             .setPhoneMac(ByteString.copyFrom(phoneMac.getBytes()))
             .setTimeStamp(minuteTime)
             .setIsCustomer(false)
             .setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes()))
             .addRssi(rssiList(rssiList.length - 1))

          reList += ((event._1, visitor.build().toByteArray))
      } else {
          val visitor = Visitor.newBuilder()
            .setSceneId(sceneId)
            .setPhoneMac(ByteString.copyFrom(phoneMac.getBytes()))
            .setTimeStamp(minuteTime)
            .setIsCustomer(false)
            .setPhoneBrand(ByteString.copyFrom(phoneBrand.getBytes()))
            .addRssi(rssiList(rssiList.length - 1))

          reList += ((event._1, visitor.build().toByteArray))
        }
    })

    reList.toIterator
  }

  def machineMacFilter(record: (String, Array[Byte])): Boolean = {
    if (record._2.isEmpty) return false
    val mac = record._1.split(Common.CTRL_A, -1)(1)
    !CommonConf.machineBrandSet.contains(mac.substring(0, 8))
  }

  def setIsCustomer(visitorIterator: Iterator[(String, Array[Byte])]): Iterator[(String, Array[Byte])] = {

    val shopRddMap = scala.collection.mutable.ListBuffer[(String, Array[Byte])]()
    val visitedCollection = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_VISITED)

    visitorIterator.foreach(record => {

      val visitor = Visitor.newBuilder().clear().mergeFrom(record._2, 0, record._2.length)
      val userMac = new String(visitor.getPhoneMac.toByteArray)
      val sceneId = visitor.getSceneId

      val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
      val todayDate = todayDateFormat.format(new Date(visitor.getTimeStamp))
      val date = todayDateFormat.parse(todayDate).getTime

      val query = new BasicDBObject
      query.put(Common.MONGO_SHOP_VISITED_DATE, date)
      query.put(Common.MONGO_SHOP_VISITED_SCENE_ID, sceneId)
      query.put(Common.MONGO_SHOP_VISITED_MAC, userMac)

      val queryCol = new BasicDBObject(Common.MONGO_SHOP_VISITED_IS_CUSTOMER, 1).append(Common.MONGO_OPTION_ID, 0)

      var isCustomer = false
      val result = visitedCollection.find(query, queryCol).toList

      if (result.nonEmpty) {
        val ret = result.head
        isCustomer = ret.get(Common.MONGO_SHOP_VISITED_IS_CUSTOMER).toString.toBoolean
      }

      visitor.setIsCustomer(isCustomer)
      shopRddMap.append((record._1, visitor.build().toByteArray))
    })

    shopRddMap.toIterator
  }


  def checkMachine(partition: Iterator[(String,Boolean, Array[Byte])]): Iterator[String]  ={

    val ret = mutable.Set[String]()

    partition.foreach(record => {
      val keyInfo = record._1.split(Common.CTRL_A, -1)
      println(keyInfo(1))
      ret.add(keyInfo(1))
    })

    ret.iterator
  }

}