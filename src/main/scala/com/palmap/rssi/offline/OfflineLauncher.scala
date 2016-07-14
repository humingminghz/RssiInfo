package com.palmap.rssi.offline


import java.text.SimpleDateFormat
import java.util.Date
import com.palmap.rssi.common.Common
import com.palmap.rssi.common.GeneralMethods
import com.palmap.rssi.funcs.{RealTimeFuncs, VisitedFuncs, HistoryFuncs}
import org.apache.hadoop.io.{BytesWritable, LongWritable}
import scala.collection.mutable
import org.apache.spark.{Partitioner, SparkContext, SparkConf}

/**
 * Created by admin on 2016/4/18.
 */
object OfflineLauncher {

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
      //.zipWithIndex().collectAsMap()
      .flatMap(UnitFuncs.visitorInfo)
      .filter(UnitFuncs.filterFuncs)
      .reduceByKey((record, nextRecord) => 1)
      .map(UnitFuncs.bulidMessage)
      .cache()
    //println(visitorRdd)

    val dayDailyRDD = visitorRdd.map(UnitFuncs.mergrVisitor)
      .aggregateByKey(List[Long]())(UnitFuncs.seqVisitor, UnitFuncs.combVisitor)
      .mapPartitions(UnitFuncs.setIsCustomer).cache()

    //history
    //((sceneId, phoneMac,phoneBrand）,(List[minuteTime],isCustomer))
    dayDailyRDD.foreachPartition(HistoryOffLine.saveHistory)

    //visited
    dayDailyRDD.foreachPartition(VisitedOffline.saveVisited)

    //(sceneId,minTine,isCustomer),mac)
    val timeFolwRDD = dayDailyRDD.mapPartitions(UnitFuncs.minVistor).cache()

    //realtime  (sceneId,minTine,isCustomer),HashSet[mac])
    timeFolwRDD.aggregateByKey(mutable.HashSet[String]())(UnitFuncs.seqminFolw, UnitFuncs.combminFolw)
     .foreachPartition(RealTimeOffline.saveRealTime _)

    //realhourTime
    timeFolwRDD.mapPartitions(UnitFuncs.hourVistor _)
      .aggregateByKey(mutable.HashSet[String]())(UnitFuncs.seqminFolw, UnitFuncs.combminFolw)
      .foreachPartition(RealTimeOffline.saveRealHourTime _)

    //day-history
    //((sceneId, phoneMac,phoneBrand）,(List[minuteTime],isCustomer))
    val dayRdd = dayDailyRDD.filter(x => (x._2._2 == true)).map(record => {
      val sceneId = record._1._1.toInt
      val time = record._2._1.head
      val sdf = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
      val dayTime = sdf.parse(sdf.format(new Date(time))).getTime
      val dwell = record._2._1.size
      (sceneId + Common.CTRL_A + dayTime, dwell)
      }).cache()

//     dayRdd.combineByKey(createCombiner = (v: Int) => (v: Int, 1),
//      mergeValue = (c: (Int, Int), v: Int) => (c._1 + 1, c._2 + v),
//      mergeCombiners = (c1: (Int, Int), c2: (Int, Int)) => (c1._1 + c1._1, c2._2 + c2._2)
//      ).map(UnitFuncs.megeDayInfo)
//       .foreachPartition(HistoryOffLine.saveDayInfo _)

    //RDD[mac]  machine
    val machineRdd = dayDailyRDD.filter(record =>
      record._2._1.size >= Common.DEFAULT_MACHINE_CHECK_MINUTE)
      .map(record => (record._1, dataDate))
      .mapPartitions(MacFilterFuncs.checkMachine _)
    machineRdd.repartition(1).saveAsTextFile(machineFilePath)


  }


}
