package com.palmap.rssi.common

import scala.collection.mutable

/**
 * Created by lingling.dai on 2016/1/12.
 */
object CommonConf {

  val sceneIdlist= mutable.HashSet[Int]()
  val businessHoursMap = scala.collection.mutable.Map[Int ,(Int, Int)]()
  ConfInfoSet.getBusinessHoursMap //get static info
  ConfInfoSet.getSceneIdlist
  ZKMonitor .startMonitor()
}
