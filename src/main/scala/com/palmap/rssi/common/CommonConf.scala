package com.palmap.rssi.common

import com.palmap.rssi.statistic.ShopSceneFuncs

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * Created by lingling.dai on 2016/1/12.
 */
object CommonConf {
  //machine set
  val machineSet = scala.collection.mutable.HashSet[String]()
  ConfInfoSet.initMachineSet(Common.MACHINE_SET_FILE)

  //init machine set
  val sceneIdlist = scala.collection.mutable.Set[Int]()
  val sceneIdMap = scala.collection.mutable.Map[Int, Int]()
  //ConfInfoSet.getSceneId(Common.SCENEID_FILE)
  ConfInfoSet.initSceneIdMap(Common.SCENE_ID_MAP)

  val businessHoursMap = scala.collection.mutable.Map[Int ,(Int, Int)]()
  ConfInfoSet.initBusinessHoursMap //get static info
//  ConfInfoSet.getSceneIdlist

  val macBrandMap = ShopSceneFuncs.getMacBrandMap(Common.MAC_BRAND)


  val machineBrandSet = mutable.HashSet[String]()
  ConfInfoSet.initMachineBrandList(Common.MACHINE_BRAND_SET_FILE)


  val apInfoMap = mutable.HashMap[Int,String]()
  ZKMonitor.startMonitor()
}
