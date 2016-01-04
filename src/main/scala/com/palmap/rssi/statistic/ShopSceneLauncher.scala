package com.palmap.rssi.statistic

import com.palmap.rssi.message.ShopStore.{ Visitor}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import java.text.SimpleDateFormat
import java.util.Date
import scala.math._
import com.google.protobuf.ByteString
import com.palmap.rssi.message.Store.{UserType}
import com.palmap.rssi.common.{GeneralMethods, Common}
import com.palmap.rssi.funcs.{RealTimeFuncs, HistoryFuncs, VisitedFuncs}
import org.apache.spark.storage.StorageLevel
import java.io.StringReader
import javax.json.Json
import scala.collection.JavaConversions._

/**
 * Created by admin on 2015/12/15.
 */
object ShopSceneLauncher {
  def main(args: Array[String]): Unit = {
    val sparkRssiInfoXml = GeneralMethods.getConf("sparkRssiInfo.xml")
    val apShopMap = ShopSceneFuncs.getApMacShopMap()
    val macBrandMap = ShopSceneFuncs.getMacBrandMap("mac_brand") //需要传递
    val (machineMap, employeeMap) = ShopSceneFuncs.getMachineAndEmployeeMac()

    val sparkConf = new SparkConf().setAppName("shopMessage")
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    val broker_list = sparkRssiInfoXml("metadata.broker.list")
    val group_id = sparkRssiInfoXml("group.id")
    val topics = sparkRssiInfoXml("topics")
    ssc.checkpoint("shoprssi-checkpoint")

    val zkQuorum = sparkRssiInfoXml("zkQuorum")
    val group = "group1"
    val numThreads = "1"
    val topicMap = Map(topics -> numThreads.toInt)

    val kafkaParams = Map[String, String]("metadata.broker.list" -> broker_list, "group.id" -> group_id)
    val messagesRdd = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(topics))
    messagesRdd.count().map(x => "Received " + x + " kafka events.").print()

    val visitorRdd = messagesRdd.map(x => x._2).filter(ShopUnitFuncs.fileterVisitor(_, apShopMap)).flatMap(x => {
      var shopRddMap = scala.collection.mutable.Map[String, Int]()
      val visitorInfo = x
      val sr = new StringReader(visitorInfo)
      val jsonObject = Json.createReader(sr).readObject()
      val apMac = jsonObject.getString("apMac").toLowerCase()
      val sceneId = jsonObject.getJsonNumber("sceneId").toString()
      val timeStamp = jsonObject.getJsonNumber("timestamp").toString()
      val rssiArray = jsonObject.getJsonArray("rssiArray")
      for (i <- 0 until rssiArray.size()) {
        val rssiobj = rssiArray.getJsonObject(i)

        val phoneMac = rssiobj.getString("clientMac")
        val rssi = rssiobj.getJsonNumber("rssi").toString().toInt
        shopRddMap.put(sceneId + "," + apMac + "," + phoneMac + "," + timeStamp, rssi)
      }
      shopRddMap
    }).reduceByKey(max).map(x => {

      val arr = x._1.split(",", -1)
      val sceneId = arr(0).toInt
      val apMac = arr(1)
      val phoneMac = arr(2).toLowerCase
      val timeStamp = arr(3).toLong // * 1000
      val rssi = x._2.toInt
      val locationId = apShopMap(apMac).toInt //获取shopId
      val visitorBuilder = Visitor.newBuilder()
      visitorBuilder.setPhoneMac(ByteString.copyFrom(phoneMac.getBytes))
      val phoneMacKey = phoneMac.substring(0, Common.MAC_KEY_LENGTH)
      val phoneBrand = Common.BRAND_UNKNOWN
      if (macBrandMap.contains(phoneMacKey)) {
        val macBrand = macBrandMap(phoneMacKey)
        visitorBuilder.setPhoneBrand(ByteString.copyFrom(macBrand.getBytes))
      }
      visitorBuilder.setSceneId(sceneId)
      visitorBuilder.setLocationId(locationId)
      visitorBuilder.addRssi(rssi)
      visitorBuilder.addTimeStamp(timeStamp)
      (phoneMac + Common.CTRL_A + locationId, visitorBuilder.build().toByteArray()) //+","+sceneId
    }).reduceByKey((bytesCurVisitor, bytesNextVisitor) => {
      val curVisitor = Visitor.newBuilder().clear().mergeFrom(bytesCurVisitor, 0, bytesCurVisitor.length)
      curVisitor.mergeFrom(bytesNextVisitor, 0, bytesNextVisitor.length)
      curVisitor.build().toByteArray()
    }).mapPartitions(ShopUnitFuncs.setUserTpe(_,machineMap,employeeMap)).cache()
    visitorRdd.print()

    //history
    visitorRdd.foreachRDD(rdd => {
      val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
      val currentDate = new Date()
      val todayDate = todayDateFormat.format(new Date(currentDate.getTime - Common.BATCH_INTERVAL_IN_MILLI_SEC))
      val date = todayDateFormat.parse(todayDate).getTime
      val currentTime = currentDate.getTime / Common.MINUTE_FORMATER * Common.MINUTE_FORMATER; //转化成秒, 封装
      HistoryFuncs.saveHistory(rdd, date, currentTime)
    })
    visitorRdd.count().map(cnt => "save data to History. " + new Date()).print()

    //Visited
    visitorRdd.foreachRDD(rdd => {

      VisitedFuncs.saveToMongo(rdd)
    })
    visitorRdd.count().map(cnt => "save data to mongo . " + new Date()).print()

    //Visited
    visitorRdd.foreachRDD(rdd => {
      var currentDate = new Date()
      val currentSec = currentDate.getTime / 1000 * 1000; //转化成秒, 封装
      currentDate = new Date(currentSec - 30 * 1000) //需要封装
      currentDate.setHours(0)
      currentDate.setMinutes(0)
      currentDate.setSeconds(0)
      val dateTime = currentDate.getTime
      VisitedFuncs.calcDwell(rdd, dateTime)
    })
    visitorRdd.count().map(cnt => "save data to Visited. " + new Date()).print()

    //RealTime
    val realTimeRdd = visitorRdd.map(record => {
      val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
      val currentDate = new Date()
      val currentTime = currentDate.getTime / Common.MINUTE_FORMATER * Common.MINUTE_FORMATER; //转化成秒, 封装
      RealTimeFuncs.calShop(record, currentTime)
    })
    val macsRdd = realTimeRdd.mapPartitions(RealTimeFuncs.mergeMacs).cache
    macsRdd.foreachRDD(RealTimeFuncs.saveMacs _)
    macsRdd.count().map(cnt => "save" + cnt + " macs data to mongo. " + new Date()).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
