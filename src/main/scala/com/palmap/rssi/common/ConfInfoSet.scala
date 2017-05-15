package com.palmap.rssi.common

import java.io.{BufferedReader, ByteArrayOutputStream, InputStreamReader}
import java.net.{HttpURLConnection, URL}
import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.{BasicDBObject, ServerAddress}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.json.JSONArray

import scala.collection.mutable
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.io.Source

/**
  * Created by lingling.dai on 2016/1/12.
  */
object ConfInfoSet {

  val todayFormat = new SimpleDateFormat(Common.TODAY_FIRST_TS_FORMAT)
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  //get machine macs set
  def getMachineSet(fileName: String): Unit = {
    Source.fromFile(fileName).getLines()
      .foreach { line =>
        val mac = line.trim()
        CommonConf.machineSet.add(mac.toUpperCase)
      }
  }

  //update machine set
  def updateMachineSet() = {
    println("before updata machine set:" + CommonConf.machineSet.size)
    val fileSystem = FileSystem.get(new Configuration())
    val in = fileSystem.open(new Path(Common.MACHINE_SET_PATH))
    val bufferedReader = new BufferedReader(new InputStreamReader(in))
    var line = ""
    while ( {
      line = bufferedReader.readLine();
      line != null
    }) {
      CommonConf.machineSet.add(line.trim)
    }
    println("after updata machine set:" + CommonConf.machineSet.size)
    if (bufferedReader != null) bufferedReader.close()
    if (in != null) in.close()
    if (fileSystem != null) fileSystem.close()
  }

  def getSceneIdlist(): Unit = {
    val url = xmlConf(Common.SHOP_SCENEIDS_URL)
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

  def getBusinessHoursMap: Unit = {
    val businessHoursColl = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_STATICINFO)
    val businessHoursList = businessHoursColl.find().toList
    if (!businessHoursList.isEmpty)
      businessHoursList.foreach(mongoDoc => {
        val sceneId = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_SCENEID).toString.toInt
        val openMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_OPENMINUTE).toString.toInt
        val closeMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_CLOSEMINUTE).toString.toInt
        CommonConf.businessHoursMap += (sceneId -> (openMinute, closeMinute))
      })
  }

  def updateBusinessHourMap(sceneId: Int): Unit = {
    val staticInfoColl = MongoFactory.getDBCollection(Common.MONGO_COLLECTION_SHOP_STATICINFO)
    val query = new BasicDBObject()
    query.put(Common.MONGO_STATICINFO_SHOP_SCENEID, sceneId)
    val staticInfoList = staticInfoColl.find(query)
    if (!staticInfoList.isEmpty)
      staticInfoList.foreach(mongoDoc => {
        val sceneId = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_SCENEID).toString.toInt
        val openMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_OPENMINUTE).toString.toInt
        val closeMinute = mongoDoc.get(Common.MONGO_STATICINFO_SHOP_CLOSEMINUTE).toString.toInt
        CommonConf.businessHoursMap += (sceneId -> (openMinute, closeMinute))
      })
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
      connection.connect
      val responseCode = connection.getResponseCode
      if (responseCode == 200) {
        return readStream(connection)
      } else {
        println("the code is : " + responseCode)
        return ""
      }
    }
    catch {
      case e: Exception => println(e.toString); ""
    } finally {
      if (reader != null) reader.close
      if (connection != null) connection.disconnect
    }
  }

  private def readStream(connection: HttpURLConnection): String = {
    val byteOut: Array[Byte] = new Array[Byte](10240)
    val outSteam: ByteArrayOutputStream = new ByteArrayOutputStream
    var len: Int = -1
    while ( {
      len = connection.getInputStream.read(byteOut);
      len != -1
    }) {
      outSteam.write(byteOut, 0, len)
    }
    outSteam.close
    return new String(outSteam.toByteArray, "utf-8")
  }

  def getMachineBrandList(fileName: String): Unit = {
    Source.fromFile(fileName).getLines().foreach(line => {
      val macBrand = line.trim
      CommonConf.machineBrandSet.add(macBrand.toUpperCase)
    })
  }

  //add by yuyingchao 20170509
  //  def getSceneIdIsCustomerTrue(fileName: String): Unit = {
  //    Source.fromFile(fileName).getLines().foreach(line => {
  //      val sceneId = line.trim
  //      // println("sceneiD: "+sceneId)
  //      CommonConf.SceneIdIsCustomerTrueSet.add(sceneId)
  //    })
  //  }
  //
  //  def getSceneId(fileName: String): Unit = {
  //    Source.fromFile(fileName).getLines().foreach(line => {
  //      val sceneIdWhite = line.trim
  //      // println("sceneiD: "+sceneIdWhite)
  //      CommonConf.sceneIdlist.add(sceneIdWhite.toInt)
  //    })
  //  }


  def getSceneIdMap(fileName: String): Unit = {
    Source.fromFile(fileName).getLines().foreach(line => {
      val eleum = line.trim.split(",")
      val sceneId = eleum(0).toInt
      val dwell = eleum(1).toInt
      //      println("sceneid: "+ eleum(0))
      //      println("dwell: "+ eleum(1))
      CommonConf.sceneIdlist.add(sceneId)
      CommonConf.sceneIdMap.put(sceneId, dwell)
    })


  }

}
