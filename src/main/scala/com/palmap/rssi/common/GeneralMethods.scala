package com.palmap.rssi.common

import scala.collection.mutable
import scala.xml.XML

object GeneralMethods {

  def getConf(path: String): mutable.Map[String, String] = {

    val xmlFile = XML.load(path)

    val confMap = (mutable.Map[String, String]() /: (xmlFile \ "property")) {
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

