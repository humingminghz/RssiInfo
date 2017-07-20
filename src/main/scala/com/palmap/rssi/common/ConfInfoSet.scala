package com.palmap.rssi.common

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.text.SimpleDateFormat

import scala.collection.mutable
import scala.io.Source
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json.JSONArray

/**
  * Created by Administrator
  */
object ConfInfoSet {

  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)

  /**
    * 程序启动时初始化机器黑名单
    * @param fileName 黑名单文件路径
    */
  def initMachineSet(fileName: String): Unit = {
    Source.fromFile(fileName).getLines()
      .foreach { line =>
        val mac = line.trim()
        CommonConf.machineSet.add(mac.toUpperCase)
      }
  }

  /**
    * zookeeper中触发黑名单变更
    *
    */
  def updateMachineSet(): Unit = {

    println("before update machine set:" + CommonConf.machineSet.size)

    val fileSystem = FileSystem.get(new Configuration())
    val in = fileSystem.open(new Path(Common.MACHINE_SET_PATH))
    val bufferedReader = new BufferedReader(new InputStreamReader(in))
    var line = ""
    // 从本地文件中读入
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

  /**
    * 从地图服务器读入场景Id
    */
  def initSceneIdList(): Unit = {

    val url = xmlConf(Common.SHOP_SCENE_IDS_URL) // 配置文件读取URL

    try {

      val result = sendGetData(url)
      val jsonList = new JSONArray(result)
      println("update sceneIdlist ")

      // 添加至内存
      for (i <- 0 until jsonList.length()) {
        CommonConf.sceneIdlist += jsonList.getInt(i)
        print("   " + jsonList.getInt(i))
      }

      print("sceneIdList: " + CommonConf.sceneIdlist)
    } catch {
      case e: Exception => println(url + "get wrong" + e.toString)
    }
  }

  /**
    * 程序启动时初始化营业时间
    */
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

  /**
    * zookeeper触发营业时间变更
    * @param sceneId 场景ID
    */
  def updateBusinessHourMap(sceneId: Int): Unit = {

    // 从mongoDB中获取营业时间
    val staticInfoColl = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_STATIC_INFO)
    val query = MongoDBObject(Common.MONGO_STATIC_INFO_SHOP_SCENE_ID -> sceneId)
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

  /**
    * 从指定URL获取response结果
    * @param get_url 目标URL
    * @return Json字符串
    */
  def sendGetData(get_url: String): String = {

    var getUrl: URL = null
    val reader: BufferedReader = null
    var connection: HttpURLConnection = null

    try {
      // 对URL获取response并返回
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

  /**
    * 根据connection 获取response
    * @param connection HttpURLConnection
    * @return Json字符串
    */
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

  /**
    * 初始化手机黑名单
    * @param fileName
    */
  def initMachineBrandList(fileName: String): Unit = {
    Source.fromFile(fileName).getLines().foreach(line => {
      val macBrand = line.trim
      CommonConf.machineBrandSet.add(macBrand.toUpperCase)
    })
  }

  /**
    * 初始化待处理的场景ID列表
    * @param fileName 场景列表地址
    */
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
