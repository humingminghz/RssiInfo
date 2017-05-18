package com.palmap.rssi.offline


import scala.collection.mutable

import com.palmap.rssi.common.Common
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import org.apache.spark.{SparkConf, SparkContext}

object OfflineLauncher {

  def main(args: Array[String]) {

    System.setProperty("spark.storage.memoryFraction", "0.4")
    System.setProperty("spark.shuffle.io.preferDirectBufs", "false")

    val inputPath = args(0)
    val dataDate = args(1).toLong
    val machineFilePath = args(2)

    val sparkConf = new SparkConf().setAppName("MergerLauncher")
    val sc = new SparkContext(sparkConf)

    val visitorRdd = sc.sequenceFile(inputPath, classOf[LongWritable], classOf[BytesWritable])
      .map(_._2)
      .flatMap(UnitFuncs.visitorInfo)
      .filter(UnitFuncs.filterFuncs)
      .reduceByKey((_, _) => 1)
      .map(UnitFuncs.buildMessage)
      .cache()

    val dayDailyRDD = visitorRdd.map(UnitFuncs.mergeVisitor)
      .aggregateByKey(List[Long]())(UnitFuncs.seqVisitor, UnitFuncs.combVisitor)
      .mapPartitions(UnitFuncs.setIsCustomer).cache()

    //history
    //((sceneId, phoneMac,phoneBrand）,(List[minuteTime],isCustomer))
    dayDailyRDD.foreachPartition(HistoryOffLine.saveHistory)

    //visited
    dayDailyRDD.foreachPartition(VisitedOffline.saveVisited)

    //(sceneId,minTine,isCustomer),mac)
    val timeFlowRDD = dayDailyRDD.mapPartitions(UnitFuncs.minVisitor).cache()

    //real time  (sceneId,minTine,isCustomer),HashSet[mac])
    timeFlowRDD.aggregateByKey(mutable.HashSet[String]())(UnitFuncs.seqMinFlow, UnitFuncs.combineFlow)
     .foreachPartition(RealTimeOffline.saveRealTime)

    //real hour time
    timeFlowRDD.mapPartitions(UnitFuncs.hourVisitor)
      .aggregateByKey(mutable.HashSet[String]())(UnitFuncs.seqMinFlow, UnitFuncs.combineFlow)
      .foreachPartition(RealTimeOffline.saveRealHourTime)

    //RDD[mac]  machine
    val machineRdd = dayDailyRDD.filter(record =>
      record._2._1.size >= Common.DEFAULT_MACHINE_CHECK_MINUTE)
      .map(record => (record._1, dataDate))
      .mapPartitions(MacFilterFuncs.checkMachine)

    machineRdd.repartition(1).saveAsTextFile(machineFilePath)
  }

}
