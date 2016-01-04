package com.palmap.rssi.statistic


import java.util.Date

import java.io.StringReader
import javax.json.Json
import scala.collection.JavaConversions._
import java.util.regex.Pattern

object ShopUnitFuncs {
  def fileterVisitor(x: String, apShopMap: Map[String, Int]): Boolean = {
    var ret = false
    try {
      val sr = new StringReader(x)
      val jsonObject = Json.createReader(sr).readObject()
      val apMac = jsonObject.getString("apMac").toLowerCase()
      if (!apShopMap.contains(apMac)) {
        ret = false
      } else {
        val sceneId = jsonObject.getJsonNumber("sceneId").toString()
        val timeStamp = jsonObject.getJsonNumber("timestamp").toString()
        val rssiArray = jsonObject.getJsonArray("rssiArray")
        for (i <- 0 until rssiArray.size()) {
          val rssiobj = rssiArray.getJsonObject(i)

          val phoneMac = rssiobj.getString("clientMac")
          val rssi = rssiobj.getJsonNumber("rssi").toString()

          if (isNumeric(timeStamp) || isNumeric(rssi) || isNumeric(sceneId)) {
            ret = false
          } else {
            val todayFormat = ShopSceneFuncs.todayFormat
            val currentDate = new Date()
            val currentTs = currentDate.getTime
            val todayDate = todayFormat.format(currentDate) //获取今天零点零时零分零秒时间戳
            val todayTs = todayFormat.parse(todayDate).getTime
            val nextDayTs = todayTs + 30 * 1000
            if (currentTs > nextDayTs && currentTs < nextDayTs + 30 * 1000) {
              ret = (timeStamp.toLong) >= todayTs //需要封装， 乘1000
            } else {
              ret = true
            }
          }
        }
      }
    } catch {
      case ex: Exception => ret = false
    }
    ret
  }

  def isNumeric(str: String): Boolean = {
    val pattern1 = Pattern.compile("[-][0-9]*")
    val pattern2 = Pattern.compile("[0-9]*")
    !(pattern1.matcher(str).matches() || pattern2.matcher(str).matches())
  }


}