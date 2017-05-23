package com.palmap.rssi.common

import com.palmap.rssi.statistic.ShopSceneFuncs

import scala.collection.mutable

/**
*  Created on 2016/1/12.
*
* @author lingling.dai
*/
object CommonConf {
  val machineSet = scala.collection.mutable.HashSet[String]()
  val sceneIdlist = scala.collection.mutable.Set[Int]()
  val sceneIdMap = scala.collection.mutable.Map[Int, Int]()
  val businessHoursMap = scala.collection.mutable.Map[Int ,(Int, Int)]()
  val macBrandMap = ShopSceneFuncs.getMacBrandMap(Common.MAC_BRAND)
  val machineBrandSet = mutable.HashSet[String]()
  val apInfoMap = mutable.HashMap[Int,String]()

  // init data
  ConfInfoSet.initMachineSet(Common.MACHINE_SET_FILE)
  ConfInfoSet.initSceneIdMap(Common.SCENE_ID_MAP)
  ConfInfoSet.initBusinessHoursMap //get static info
  ConfInfoSet.initMachineBrandList(Common.MACHINE_BRAND_SET_FILE)
  ZKMonitor.startMonitor()
}
