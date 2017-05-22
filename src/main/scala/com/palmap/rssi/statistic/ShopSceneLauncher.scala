package com.palmap.rssi.statistic

import com.palmap.rssi.common.{Common, GeneralMethods}
import com.palmap.rssi.funcs._
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

    val sparkConf = new SparkConf().setAppName("frost-launcher")
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
    messagesRdd.count().map(x => s"Received ${x} kafka events. ${System.currentTimeMillis()}").print()

    val preVisitorRdd = messagesRdd.map(_._2)
      .flatMap(ShopUnitFuncs.rssiInfo)
      .mapPartitions(ShopUnitFuncs.filterBusinessVisitor)
      .reduceByKey((record, nextRecord) => { // 添加Connected 字段判断, 只要有一次链接成功 就认为是true
        if(record._3 || nextRecord._3){
          (record._1 ++ nextRecord._1,  record._2 ++ nextRecord._2, true)
        }else {
          (record._1 ++ nextRecord._1,  record._2 ++ nextRecord._2, false)
        } }).cache()

    val visitorRdd = preVisitorRdd
      .mapPartitions(ShopUnitFuncs.buildVisitor)
      .filter(!_._2.isEmpty).cache()
    // .filter(ShopUnitFuncs.machineMacFilter)   mac黑名单过滤提前到 ShopUnitFuncs.rssiInfo 中， filter更改为Visitor不为空

    val connectionsRdd = preVisitorRdd
      .mapPartitions(ConnectionsFuncs.calConnections)
      .reduceByKey((record, nextRecord) => record ++ nextRecord) // 将同一个 sceneId + timeStamp 的value合并
      .cache()

    preVisitorRdd.foreachRDD(_.unpersist(false))

    connectionsRdd.foreachRDD(ConnectionsFuncs.saveConnections _)
    connectionsRdd.count().map(x => s"process ${x} data: saved data to shop Connection ${System.currentTimeMillis()}").print()

    connectionsRdd.foreachRDD(_.unpersist(false))

    //history
    visitorRdd.foreachRDD(HistoryFuncs.saveHistory _)
    visitorRdd.count().map(x => s"process ${x} data; save data to History. ${System.currentTimeMillis()}").print()


    //Visited  calcDwellIsCustomer
    val realTimeRdd = visitorRdd
      .mapPartitions(VisitedFuncs.calVisitorDwell)
      .mapPartitions(RealTimeFuncs.calRealTime)
      .reduceByKey((record, nextRecord) => record ++ nextRecord)
      .cache()

    visitorRdd.foreachRDD(_.unpersist(false)) // realTimeRdd 计算后将visitorRdd从缓存中释放

    realTimeRdd.count().map(x => s"process ${x} data; save data to calVisitorDwell. ${System.currentTimeMillis()}").print()
    realTimeRdd.foreachRDD(RealTimeFuncs.saveRealTime _)
    realTimeRdd.count().map(x => s"process ${x} data; save data to realTime. ${System.currentTimeMillis()}").print()

    realTimeRdd.foreachRDD(_.unpersist(false))

    ssc.start()
    ssc.awaitTermination()
  }
}