package com.palmap.rssi.common

import scala.xml.XML
import scala.collection.mutable.Map
import java.util.Properties
import scala.collection.JavaConverters._
import java.io.FileInputStream
import scala.io.Source
object GeneralMethods {
  def getConf(path: String): Map[String, String] = {
    val xmlFile = XML.load(path) // 加载XML文件 
    val confMap = (Map[String, String]() /: (xmlFile \ "property")) {
      (map, bookNode) =>
        {
          val name = (bookNode \ "name").text.toString
          val value = (bookNode \ "value").text.toString
          map(name) = value
          map
        }
    }
    confMap
  }
}