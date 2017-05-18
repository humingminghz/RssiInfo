package com.palmap.rssi.statistic

import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.io.Source

import com.palmap.rssi.common.{Common, GeneralMethods}

object ShopSceneFuncs {

  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def getMacBrandMap(fileName: String): Map[String, String] = {

    val macBrandMap = scala.collection.mutable.Map[String, String]()

    Source.fromFile(fileName).getLines()
      .filter(_.split(Common.CTRL_A).length == 2)
      .foreach { line =>
        val arr = line.split(Common.CTRL_A)
        macBrandMap(arr(0)) = arr(1)
      }

    macBrandMap.toMap
  }

}
