package com.palmap.rssi.statistic

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{MongoFactory, Common, GeneralMethods}
import com.palmap.rssi.funcs.{HistoryFuncs, RealTimeFuncs, VisitedFuncs}
import com.palmap.rssi.message.ShopStore.Visitor
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.math._

/**
 * Created by admin on 2015/12/15.
 */
object ShopSceneLauncher {
  def main(args: Array[String]): Unit = {
    val sparkRssiInfoXml = GeneralMethods.getConf(Common.SPARK_CONFIG)
    val broker_list = sparkRssiInfoXml(Common.KAFKA_METADATA_BROKER)
    val group_id = sparkRssiInfoXml(Common.SPARK_GROUP_ID)
    val topics = sparkRssiInfoXml(Common.SPARK_TOPICS)
    System.setProperty("spark.storage.memoryFraction", "0.4")
    System.setProperty("spark.shuffle.io.preferDirectBufs", "false")

    val machineFilePath = args(0)

    val sparkConf = new SparkConf().setAppName("frost-launcher")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
   // sparkConf.set("spark.kryoserializer.buffer.mb","5")
    //开启shuffle block file的合并
    sparkConf.set("spark.shuffle.consolidateFiles", "true")

    sparkConf.registerKryoClasses(Array(classOf[Visitor], classOf[Visitor.Builder]))
    val ssc = new StreamingContext(sparkConf, Seconds(60))
    ssc.checkpoint("frost-checkpoint")
    val kafkaParams = Map[String, String](Common.KAFKA_METADATA_BROKER -> broker_list, Common.SPARK_GROUP_ID -> group_id)
    val messagesRdd = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, Set(topics))
    messagesRdd.count().map(x => "Received " + x + " kafka events.").print()

    val visitorRdd = messagesRdd.map(_._2)
      .flatMap(ShopUnitFuncs.visitorInfo1)
      .mapPartitions(ShopUnitFuncs.filterBusinessVisitor)
      .reduceByKey((record, nextRecord) => record ++ nextRecord)
      .mapPartitions(ShopUnitFuncs.buildVisitor1)
      .cache()

//    //.repartition(System.getProperty("spark.default.parallelism").toInt)
//    val visitorRdd = messagesRdd.map(x => x._2)
//    .flatMap(ShopUnitFuncs.visitorInfo _)
//    .filter(ShopUnitFuncs.filterFuncs _)
//    .reduceByKey(max)
//    .mapPartitions(ShopUnitFuncs.buildVisitor _)
//    .reduceB yKey((bytesCurVisitor, bytesNextVisitor) => {
//      val curVisitor = Visitor.newBuilder().clear().mergeFrom(bytesCurVisitor, 0, bytesCurVisitor.length)
//      curVisitor.mergeFrom(bytesNextVisitor, 0, bytesNextVisitor.length)
//      curVisitor.build().toByteArray()
//    })
//      //.filter(record => ShopUnitFuncs.visitorFilter(record._2))
//     .cache()

    //history
    visitorRdd.foreachRDD(HistoryFuncs.saveHistory _)
    visitorRdd.count().map("process " + _ + " data; save data to History. " + new Date()).print

    val visitorDwellRDD= visitorRdd.mapPartitions(VisitedFuncs.calVisitorDwell _).cache()
    //Visited  calcDwellIsCustomer
    val realTimeRdd = visitorDwellRDD.mapPartitions(RealTimeFuncs.calRealTime _)
      .reduceByKey((record, nextRecord) => record ++ nextRecord)
      .cache()
    realTimeRdd.count().map("process " +_ + " data; save data to calVisitorDwell. " + new Date()).print

    realTimeRdd.foreachRDD(RealTimeFuncs.saveRealTime _)
    realTimeRdd.count().map("process " +_ + " data; save data to realTime. " + new Date()).print



    ssc.start()
    ssc.awaitTermination()
  }
}
