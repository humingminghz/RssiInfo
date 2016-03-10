package com.palmap.rssi.statistic

import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.BasicDBObject
import com.palmap.rssi.common.{MongoFactory, Common, GeneralMethods}
import com.palmap.rssi.funcs.{HistoryFuncs, RealTimeFuncs, VisitedFuncs}
import com.palmap.rssi.message.ShopStore.Visitor
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
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

    val sparkConf = new SparkConf().setAppName("frost-test-launcher")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.kryoserializer.buffer.mb","5")
    sparkConf.registerKryoClasses(Array(classOf[Visitor], classOf[Visitor.Builder]))

    //sparkConf.set("spark.default.parallelism", "60")

    //开启shuffle block file的合并
    sparkConf.set("spark.shuffle.consolidateFiles", "true")

    val ssc = new StreamingContext(sparkConf, Seconds(60))
    ssc.checkpoint("frost-checkpoint")
    val kafkaParams = Map[String, String](Common.KAFKA_METADATA_BROKER -> broker_list, Common.SPARK_GROUP_ID -> group_id)
    val messagesRdd = KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, Set(topics))
    messagesRdd.count().map(x => "Received " + x + " kafka events.").print()

    val visitorRdd = messagesRdd.map(x => x._2)
    .flatMap(ShopUnitFuncs.visitorInfo _)
    .reduceByKey(max)
    .mapPartitions(ShopUnitFuncs.buildVisitor _)
    .reduceByKey((bytesCurVisitor, bytesNextVisitor) => {
      val curVisitor = Visitor.newBuilder().clear().mergeFrom(bytesCurVisitor, 0, bytesCurVisitor.length)
      curVisitor.mergeFrom(bytesNextVisitor, 0, bytesNextVisitor.length)
      curVisitor.build().toByteArray()
    })
      .filter(record => ShopUnitFuncs.visitorFilter(record._2))
     .cache()


    //history
    visitorRdd.foreachRDD(HistoryFuncs.saveHistory _)
    visitorRdd.count().map(cnt => "save data to History. " + new Date()).print()
    //Visited
    visitorRdd.foreachRDD(VisitedFuncs.calcDwell _)
    visitorRdd.count().map(cnt => "save data to Visited. " + new Date()).print()

    val realTimeRdd = visitorRdd .mapPartitions(ShopUnitFuncs.setIsCustomer _).mapPartitions(RealTimeFuncs.calRealTime _)
    realTimeRdd.map(RealTimeFuncs.saveMacs(_)).count().map(cnt=>("saveRealtimeRdd is ok"+new Date())).print()


  }
}
