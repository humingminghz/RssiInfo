package com.palmap.rssi.common

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by Mingming Hu on 2017/5/23.
  *
  */
object DateUtil {

  def getDayTimeStamp(mills:Long) : Long ={
    val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
    val date = new Date(mills)
    val dayString = todayDateFormat.format(date)
    todayDateFormat.parse(dayString).getTime()
  }

  def getHourTimeStamp(mills:Long) : Long ={
    val toHourDateFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
    val date = new Date(mills)
    val hourString = toHourDateFormat.format(date)
    toHourDateFormat.parse(hourString).getTime()
  }

  def getMinuteTimeStamp(mills:Long) : Long ={
    val toMinuteDateFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
    val date = new Date(mills)
    val minuteString = toMinuteDateFormat.format(date)
    toMinuteDateFormat.parse(minuteString).getTime()
  }


}
