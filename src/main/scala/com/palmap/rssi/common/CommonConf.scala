package com.palmap.rssi.common

import com.palmap.rssi.statistic.ShopSceneFuncs

import scala.collection.mutable

/**
*  Created on 2016/1/12.
*
* @author lingling.dai
*/
object CommonConf {
  val machineSet: mutable.HashSet[String] = scala.collection.mutable.HashSet[String]()
  val sceneIdlist: mutable.Set[Int] = scala.collection.mutable.Set[Int]()
  val sceneIdMap: mutable.Map[Int, Int] = scala.collection.mutable.Map[Int, Int]()
  val businessHoursMap: mutable.Map[Int, (Int, Int)] = scala.collection.mutable.Map[Int ,(Int, Int)]()
  val macBrandMap: Map[String, String] = ShopSceneFuncs.getMacBrandMap(Common.MAC_BRAND)
  val machineBrandSet: mutable.HashSet[String] = mutable.HashSet[String]()
  val apInfoMap: mutable.HashMap[Int, String] = mutable.HashMap[Int,String]()

  // init data
  ConfInfoSet.initMachineSet(Common.MACHINE_SET_FILE)
  ConfInfoSet.initSceneIdMap(Common.SCENE_ID_MAP)
  ConfInfoSet.initBusinessHoursMap() //get static info
  ConfInfoSet.initMachineBrandList(Common.MACHINE_BRAND_SET_FILE)
  ZKMonitor.startMonitor()
}
