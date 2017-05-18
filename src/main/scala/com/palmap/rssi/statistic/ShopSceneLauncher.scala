package com.palmap.rssi.statistic
import java.util.Date

import com.palmap.rssi.common.{Common, GeneralMethods}
import com.palmap.rssi.funcs.{HistoryFuncs, RealTimeFuncs, VisitedFuncs}
import com.palmap.rssi.message.ShopStore.Visitor
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
  * 程序入口
  */
object ShopSceneLauncher {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkRssiInfoXml = GeneralMethods.getConf(Common.SPARK_CONFIG)
    val broker_list = sparkRssiInfoXml(Common.KAFKA_METADATA_BROKER)
    val group_id = sparkRssiInfoXml(Common.SPARK_GROUP_ID)
    val topics = sparkRssiInfoXml(Common.SPARK_TOPICS)

    System.setProperty("spark.storage.memoryFraction", "0.4")
    System.setProperty("spark.shuffle.io.preferDirectBufs", "false")

    val sparkConf = new SparkConf().setAppName("frost-launcher").setMaster("local[*]")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.shuffle.consolidateFiles", "true") // 开启shuffle block file的合并
    sparkConf.registerKryoClasses(Array(classOf[Visitor], classOf[Visitor.Builder]))
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(60))
    ssc.checkpoint("frost-checkpoint")

    val kafkaParams =
      Map[String, String](Common.KAFKA_METADATA_BROKER -> broker_list, Common.SPARK_GROUP_ID -> group_id)
    val messagesRdd =
      KafkaUtils.createDirectStream[String, Array[Byte], StringDecoder, DefaultDecoder](ssc, kafkaParams, Set(topics))
    messagesRdd.count().map(x => "Received " + x + " kafka events.").print()

    val visitorRdd = messagesRdd.map(_._2)
      .flatMap(ShopUnitFuncs.visitorInfo)
      .mapPartitions(ShopUnitFuncs.filterBusinessVisitor)
      .reduceByKey((record, nextRecord) => {
        (record._1 ++ nextRecord._1,  record._2 ++ nextRecord._2)})
      .mapPartitions(ShopUnitFuncs.buildVisitor)
      .filter(ShopUnitFuncs.machineMacFilter)
      .cache()

    //history
    visitorRdd.foreachRDD(HistoryFuncs.saveHistory _)
    visitorRdd.count().map("process " + _ + " data; save data to History. " + new Date()).print
    visitorRdd.foreachRDD(_.unpersist())

    //Visited  calcDwellIsCustomer
    val realTimeRdd = visitorRdd
      .mapPartitions(VisitedFuncs.calVisitorDwell)
      .mapPartitions(RealTimeFuncs.calRealTime)
      .reduceByKey((record, nextRecord) => record ++ nextRecord)
      .cache()
    realTimeRdd.count().map("process " +_ + " data; save data to calVisitorDwell. " + new Date()).print
    realTimeRdd.foreachRDD(RealTimeFuncs.saveRealTime _)
    realTimeRdd.count().map("process " +_ + " data; save data to realTime. " + new Date()).print

    realTimeRdd.foreachRDD(_.unpersist())

    ssc.start()
    ssc.awaitTermination()
  }
}
