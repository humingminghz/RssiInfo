package com.palmap.rssi.offline


import java.util.Date
import com.palmap.rssi.common.Common
import com.palmap.rssi.common.GeneralMethods
import com.palmap.rssi.funcs.{RealTimeFuncs, VisitedFuncs, HistoryFuncs}
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import scala.collection.mutable
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by admin on 2016/4/18.
 */
object ShopSceneLauncher {

  def main(args: Array[String]) {
    val sparkRssiInfoXml = GeneralMethods.getConf("sparkRssiInfo.xml")

    System.setProperty("spark.storage.memoryFraction", "0.4")
    System.setProperty("spark.shuffle.io.preferDirectBufs", "false")
    val inputPath = args(0)
    val dataDate = args(1).toLong
    val machineFilePath = args(2)

    val sparkConf = new SparkConf().setAppName("MergerLauncher")

    val sc = new SparkContext(sparkConf)

    val visitorRdd = sc.sequenceFile(inputPath, classOf[LongWritable], classOf[BytesWritable])
      .map(_._2)
      .flatMap(UnitFuncs.visitorInfo1)
      .filter(UnitFuncs.filterFuncs)
      .reduceByKey((record, nextRecord) => 1)
      .map(UnitFuncs.bulidMessage)
      .cache()

    val dayDailyRDD=visitorRdd.map(UnitFuncs.mergrVisitor(_))
      .aggregateByKey(List[Long]())(UnitFuncs.seqVisitor, UnitFuncs.combVisitor)
      .mapPartitions(UnitFuncs.setIsCustomer _).cache()

    //history
    dayDailyRDD.foreachPartition(HistoryOffLine.saveHistory _)

    //visited
    dayDailyRDD.foreachPartition(VisitedOffline.saveVisited _)

    //(sceneId,minTine,isCustomer),mac)
    val minFolwRDD=dayDailyRDD.mapPartitions(UnitFuncs.minVistor _).cache()

    //realtime  (sceneId,minTine,isCustomer),HashSet[mac])
     minFolwRDD.aggregateByKey(mutable.HashSet[String]())(UnitFuncs.seqminFolw, UnitFuncs.combminFolw)
     .foreachPartition(RealTimeOffline.saveRealTime _)

    //realhourTime
    minFolwRDD.mapPartitions(UnitFuncs.hourVistor _)
      .aggregateByKey(mutable.HashSet[String]())(UnitFuncs.seqminFolw, UnitFuncs.combminFolw)
    .foreachPartition(RealTimeOffline.saveRealHourTime _)

    //RDD[mac]  machine
    val machineRdd = dayDailyRDD.filter(record =>
      record._2._1.size >= Common.DEFAULT_MACHINE_CHECK_MINUTE)
      .map(record => (record._1, dataDate))
      .mapPartitions(MacFilterFuncs.checkMachine _)
    machineRdd.saveAsTextFile(machineFilePath)


    print("process " + _ + " data; save data to History. " + new Date())

  }


}
