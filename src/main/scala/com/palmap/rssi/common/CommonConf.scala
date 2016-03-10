package com.palmap.rssi.common

/**
 * Created by lingling.dai on 2016/1/12.
 */
object CommonConf {


  val businessHoursMap = scala.collection.mutable.Map[Int ,(Int, Int)]()
  ConfInfoSet.getBusinessHoursMap //get static info
  ZKMonitor .startMonitor()
}
