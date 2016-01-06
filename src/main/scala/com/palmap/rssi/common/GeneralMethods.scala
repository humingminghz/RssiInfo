package com.palmap.rssi.common

import scala.collection.mutable.Map
import scala.xml.XML

object GeneralMethods {
  def getConf(path: String): Map[String, String] = {
    val xmlFile = XML.load(path)
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