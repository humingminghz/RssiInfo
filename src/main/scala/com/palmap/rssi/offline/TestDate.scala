package com.palmap.rssi.offline

import java.text.SimpleDateFormat
import java.util.Date

import com.palmap.rssi.common.Common

/**
 * Created by shenyue.ma on 2016/8/10.
 */
object TestDate {
  def main(args: Array[String]) {
    val sdf = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
//    val dataDate = sdf.format(new Date("2016-08-03 00:00:00"))
    val ts = sdf.parse("2016-08-03 00:00:00")
    println(ts.getTime)

  }

}
