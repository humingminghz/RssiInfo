package com.palmap.rssi.common

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.io.Source

import com.mongodb.BasicDBObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json.JSONArray

/**
  * Created by lingling.dai on 2016/1/12.
  */
object ConfInfoSet {

  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)

  //get machine macs set
  def initMachineSet(fileName: String): Unit = {
    Source.fromFile(fileName).getLines()
      .foreach { line =>
        val mac = line.trim()
        CommonConf.machineSet.add(mac.toUpperCase)
      }
  }

  //update machine set
  def updateMachineSet(): Unit = {

    println("before update machine set:" + CommonConf.machineSet.size)

    val fileSystem = FileSystem.get(new Configuration())
    val in = fileSystem.open(new Path(Common.MACHINE_SET_PATH))
    val bufferedReader = new BufferedReader(new InputStreamReader(in))
    var line = ""

    while ( {
      line = bufferedReader.readLine()
      line != null
    }) {
      CommonConf.machineSet.add(line.trim)
    }
    println("after update machine set:" + CommonConf.machineSet.size)

    if (bufferedReader != null) {
      bufferedReader.close()
    }

    if (in != null) {
      in.close()
    }

    if (fileSystem != null) {
      fileSystem.close()
    }

  }

  def initSceneIdList(): Unit = {

    val url = xmlConf(Common.SHOP_SCENE_IDS_URL)

    try {

      val result = sendGetData(url)
      val jsonList = new JSONArray(result)
      println("update sceneIdlist ")

      for (i <- 0 until jsonList.length()) {
        CommonConf.sceneIdlist += jsonList.getInt(i)
        print("   " + jsonList.getInt(i))
      }

      print("sceneIdList: " + CommonConf.sceneIdlist)
    } catch {
      case e: Exception => println(url + "get wrong" + e.toString)
    }
  }

  def initBusinessHoursMap(): Unit = {

    val businessHoursColl = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_STATIC_INFO)
    val businessHoursList = businessHoursColl.find().toList

    if (businessHoursList.nonEmpty){
      businessHoursList.foreach(mongoDoc => {
        val sceneId = mongoDoc.get(Common.MONGO_STATIC_INFO_SHOP_SCENE_ID).toString.toInt
        val openMinute = mongoDoc.get(Common.MONGO_STATIC_INFO_SHOP_OPEN_MINUTE).toString.toInt
        val closeMinute = mongoDoc.get(Common.MONGO_STATIC_INFO_SHOP_CLOSE_MINUTE).toString.toInt
        CommonConf.businessHoursMap += (sceneId -> (openMinute, closeMinute))
      })
    }

  }

  def updateBusinessHourMap(sceneId: Int): Unit = {

    val staticInfoColl = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_STATIC_INFO)
    val query = new BasicDBObject()
    query.put(Common.MONGO_STATIC_INFO_SHOP_SCENE_ID, sceneId)
    val staticInfoList = staticInfoColl.find(query)

    if (staticInfoList.nonEmpty) {
      staticInfoList.foreach(mongoDoc => {
        val sceneId = mongoDoc.get(Common.MONGO_STATIC_INFO_SHOP_SCENE_ID).toString.toInt
        val openMinute = mongoDoc.get(Common.MONGO_STATIC_INFO_SHOP_OPEN_MINUTE).toString.toInt
        val closeMinute = mongoDoc.get(Common.MONGO_STATIC_INFO_SHOP_CLOSE_MINUTE).toString.toInt
        CommonConf.businessHoursMap += (sceneId -> (openMinute, closeMinute))
      })
    }

  }

  def sendGetData(get_url: String): String = {

    var getUrl: URL = null
    val reader: BufferedReader = null
    var connection: HttpURLConnection = null

    try {

      getUrl = new URL(get_url)
      connection = getUrl.openConnection.asInstanceOf[HttpURLConnection]
      connection.addRequestProperty("Accept", "application/json")
      connection.setConnectTimeout(30000)
      connection.setReadTimeout(30000)
      connection.connect()
      val responseCode = connection.getResponseCode

      if (responseCode == 200) {
        readStream(connection)
      } else {
        println("the code is : " + responseCode)
        ""
      }
    } catch {
      case e: Exception => println(e.toString); ""
    } finally {

      if (reader != null) {
        reader.close()
      }

      if (connection != null) {
        connection.disconnect()
      }

    }
  }

  private def readStream(connection: HttpURLConnection): String = {

    val byteOut: Array[Byte] = new Array[Byte](10240)
    val outSteam: ByteArrayOutputStream = new ByteArrayOutputStream
    var len: Int = -1

    while ( {
      len = connection.getInputStream.read(byteOut)
      len != -1
    }) {
      outSteam.write(byteOut, 0, len)
    }
    outSteam.close()

    new String(outSteam.toByteArray, "utf-8")
  }

  def initMachineBrandList(fileName: String): Unit = {
    Source.fromFile(fileName).getLines().foreach(line => {
      val macBrand = line.trim
      CommonConf.machineBrandSet.add(macBrand.toUpperCase)
    })
  }

  def initSceneIdMap(fileName: String): Unit = {

    Source.fromFile(fileName).getLines().foreach(line => {

      val elem = line.trim.split(",")
      val sceneId = elem(0).toInt
      val dwell = elem(1).toInt
      CommonConf.sceneIdlist.add(sceneId)
      CommonConf.sceneIdMap.put(sceneId, dwell)

    })

  }

}
