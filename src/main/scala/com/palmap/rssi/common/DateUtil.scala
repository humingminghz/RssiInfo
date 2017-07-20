package com.palmap.rssi.common

import java.text.SimpleDateFormat
import java.util.Date

/**
  * Created by Mingming Hu
  * Date相关操作的方法集合
  */
object DateUtil {

  /**
    * 将毫秒数据转换为日期级别的数据
    * @param mills 传入时间nano Seconds
    * @return day级别的时间
    */
  def getDayTimestamp(mills:Long) : Long ={
    val todayDateFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
    val date = new Date(mills)
    val dayString = todayDateFormat.format(date)
    todayDateFormat.parse(dayString).getTime()
  }

  /**
    * 将毫秒级数据转换为小时级别的数据
    * @param mills 传入时间nano Seconds
    * @return hour级别的时间
    */
  def getHourTimestamp(mills:Long) : Long ={
    val toHourDateFormat = new SimpleDateFormat(Common.NOW_HOUR_FORMAT)
    val date = new Date(mills)
    val hourString = toHourDateFormat.format(date)
    toHourDateFormat.parse(hourString).getTime()
  }

  /**
    * 将毫秒级数据转换为分钟级别的数据
    * @param mills 传入时间nano Seconds
    * @return minute级别的时间
    */
  def getMinuteTimestamp(mills:Long) : Long ={
    val toMinuteDateFormat = new SimpleDateFormat(Common.NOW_MINUTE_FORMAT)
    val date = new Date(mills)
    val minuteString = toMinuteDateFormat.format(date)
    toMinuteDateFormat.parse(minuteString).getTime()
  }


}
