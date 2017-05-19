package com.palmap.rssi.statistic
import java.util.Date

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
    messagesRdd.count().map(x => "Received " + x + " kafka events.").print()

    val preVisitorRdd = messagesRdd.map(_._2)
      .flatMap(ShopUnitFuncs.rssiInfo)
      .mapPartitions(ShopUnitFuncs.filterBusinessVisitor)
      .reduceByKey((record, nextRecord) => { // 添加Connected 字段判断, 只要有一次链接成功 就认为是true
        if(record._3 || nextRecord._3){
          (record._1 ++ nextRecord._1,  record._2 ++ nextRecord._2, true)
        }else {
          (record._1 ++ nextRecord._1,  record._2 ++ nextRecord._2, false)
        } }).cache()

    preVisitorRdd.count().map("preVisitorRdd: " + _).print()

    val visitorRdd = preVisitorRdd
      .mapPartitions(ShopUnitFuncs.buildVisitor)
      .filter(!_._2.isEmpty).cache()
    // .filter(ShopUnitFuncs.machineMacFilter)   mac黑名单过滤提前到 ShopUnitFuncs.rssiInfo 中， filter更改为Visitor不为空
//    preVisitorRdd.foreachRDD(rdd => {
//      rdd.map(x => println("x._1: " + x._1)).count()
//      rdd
//    })
//    preVisitorRdd.filter(record => record._1.contains(Common.SCENE_ID_HUAWEI.toString) && (record._2._3 == true))
//      .foreachRDD(rdd => ConnectionsFuncs.saveConnections _) // 将华为的connection 信息存到MongoDB

    preVisitorRdd.foreachRDD( rdd => rdd.foreachPartition( x => x.foreach(x => println(x))))
    preVisitorRdd.foreachRDD( rdd => ConnectionsFuncs.saveConnections _) // 保存至mongo  目前只算华为id 以及connected是true的

    preVisitorRdd.count().map("process " + _ + " data: saved data to shop Connection " + new Date()).print()


    preVisitorRdd.foreachRDD(_.unpersist(false))

    //history
    visitorRdd.foreachRDD(HistoryFuncs.saveHistory _)
    visitorRdd.count().map("process " + _ + " data; save data to History. " + new Date()).print


    //Visited  calcDwellIsCustomer
    val realTimeRdd = visitorRdd
      .mapPartitions(VisitedFuncs.calVisitorDwell)
      .mapPartitions(RealTimeFuncs.calRealTime)
      .reduceByKey((record, nextRecord) => record ++ nextRecord)
      .cache()

    visitorRdd.foreachRDD(_.unpersist(false)) // realTimeRdd 计算后将visitorRdd从缓存中释放

    realTimeRdd.count().map("process " +_ + " data; save data to calVisitorDwell. " + new Date()).print
    realTimeRdd.foreachRDD(RealTimeFuncs.saveRealTime _)
    realTimeRdd.count().map("process " +_ + " data; save data to realTime. " + new Date()).print

    realTimeRdd.foreachRDD(_.unpersist())

    ssc.start()
    ssc.awaitTermination()
  }
}
