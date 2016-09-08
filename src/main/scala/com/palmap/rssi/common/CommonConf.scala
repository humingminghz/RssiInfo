package com.palmap.rssi.common

import com.palmap.rssi.statistic.ShopSceneFuncs

import scala.collection.mutable

/**
 * Created by lingling.dai on 2016/1/12.
 */
object CommonConf {
  //machine set
  val machineSet = scala.collection.mutable.HashSet[String]()
  ConfInfoSet.getMachineSet(Common.MACHINE_SET_FILE)
  //init machine set
  val sceneIdlist = scala.collection.mutable.Set[Int](11301)
  val businessHoursMap = scala.collection.mutable.Map[Int ,(Int, Int)]()
  ConfInfoSet.getBusinessHoursMap //get static info
  ConfInfoSet.getSceneIdlist

  val macBrandMap = ShopSceneFuncs.getMacBrandMap(Common.MAC_BRAND)

  val machineBrandSet = mutable.HashSet[String]()
  ConfInfoSet.getMachineBrandList(Common.MACHINEBRAND_SET_FILE)

  ZKMonitor.startMonitor()
}
