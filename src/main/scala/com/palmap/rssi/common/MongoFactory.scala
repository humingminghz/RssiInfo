package com.palmap.rssi.common

import scala.collection.mutable

import com.mongodb.{MongoClientOptions, MongoCredential, ServerAddress}
import com.mongodb.casbah.{MongoClient, MongoCollection, MongoDB}
import scala.collection.mutable.ListBuffer


/**
  *  mongoDB的connection类
  *
 */
object MongoFactory {

  val appConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)

  val host: List[String] = appConf(Common.MONGO_ADDRESS_LIST).split(",", -1).toList
  val port: Int = appConf(Common.MONGO_SERVER_PORT).toInt
  val dbName: String = appConf(Common.MONGO_DB_NAME)
  val connectionsPerHost: Int = appConf(Common.MONGO_SERVER_CONNECTIONS_PER_HOST).toInt
  val threadsAllowedToBlockForConnectionMultiplier: Int = appConf(Common.MONGO_SERVER_THREADS).toInt
  val user: String = appConf(Common.MONGO_SERVER_USER)
  val pwd: String = appConf(Common.MONGO_SERVER_PWD)
  val authenticate: Boolean = appConf(Common.MONGO_SERVER_AUTHENTICATE).toBoolean

  var mongo: MongoClient = _

  /**
    * 初始化mongoDB
    */
  def init(): Unit = {

    if (host == null || host.isEmpty) {
      throw new NumberFormatException("host is null")
    }

    if (port == 0) {
      throw new NumberFormatException("port is null")
    }

    try {

      val serverList = ListBuffer[ServerAddress]()

      for (i <- host.indices) {
        val sa = new ServerAddress(host(i), port)
        serverList.append(sa)
      }

      //connection pool config
      val options = MongoClientOptions.builder()
        .connectionsPerHost(connectionsPerHost)
        .threadsAllowedToBlockForConnectionMultiplier(threadsAllowedToBlockForConnectionMultiplier)
        .build()
      val credential = MongoCredential.createCredential(user, dbName, pwd.toCharArray)
      val credentialList = List[MongoCredential](credential)
      //authenticate
      if (authenticate) {
        mongo = MongoClient(serverList.toList, credentialList, options)
      } else {
        mongo = MongoClient(serverList.toList, options)
      }
    } catch {
      case e: Exception => println("ERROR mongo:  " + e.printStackTrace())
    }
  }

  init()

  val db: MongoDB = mongo.getDB(dbName)

  /**
    * 获取指定collection
    * @param tableName collection 名称
    * @return MongoCollection
    */
  def getDBCollection(tableName: String): MongoCollection = {
    MongoFactory.db(tableName)
  }

}
