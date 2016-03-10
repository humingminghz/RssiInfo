package com.palmap.rssi.statistic

import java.text.SimpleDateFormat

import com.palmap.rssi.common.{MongoFactory, GeneralMethods, Common}

import scala.collection.JavaConversions._
import scala.io.Source

object ShopSceneFuncs {
  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  def getMacBrandMap(fileName: String): Map[String, String] = {
    var macBrandMap = scala.collection.mutable.Map[String, String]()
    Source.fromFile(fileName).getLines()
      .filter(_.split(Common.CTRL_A).length == 2)
      .foreach { line =>
        val arr = line.split(Common.CTRL_A)
        macBrandMap(arr(0)) = arr(1)
      }
    macBrandMap.toMap
  }

}
