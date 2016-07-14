package com.palmap.rssi.common

import com.mongodb.{MongoCredential, MongoClientOptions, ServerAddress}
import com.mongodb.casbah.{MongoCollection, MongoClient}

import scala.collection.mutable.ListBuffer


/**
 * Created by fengchao.wang on 2016/2/22.
 */
object MongoFactory {

  val appConf = GeneralMethods.getConf(Common.SPARK_CONFIG)

  val host = appConf(Common.MONGO_ADDRESS_LIST).split(",", -1).toList
  val port = appConf(Common.MONGO_SERVER_PORT).toInt
  val dbName = appConf(Common.MONGO_DB_NAME)
  val connectionsPerHost = appConf(Common.MONGO_SERVER_CONNECTIONSPERHOST).toInt
  val threadsAllowedToBlockForConnectionMultiplier = appConf(Common.MONGO_SERVER_THREADS).toInt
  val user = appConf(Common.MONGO_SERVER_USER)
  val pwd = appConf(Common.MONGO_SERVER_PWD)
  val authenticate = appConf(Common.MONGO_SERVER_AUTHENTICATE).toBoolean

  var mongo: MongoClient = null

  def init() = {
    if (host == null || host.isEmpty) {
      throw new NumberFormatException("host is null")
    }
    if (port == 0) {
      throw new NumberFormatException("port is null")
    }
    try {
      val serverList = ListBuffer[ServerAddress]()
      for (i <- 0 until host.length) {
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
      if (authenticate) mongo = MongoClient(serverList.toList, credentialList, options)
      //no authenticate
      else mongo = MongoClient(serverList.toList, options)
    } catch {
      case e: Exception => println("ERROR mongo:  " + e.printStackTrace())
    }
  }

  init()

  val db = mongo.getDB(dbName)

  def getDBCollection(tableName: String): MongoCollection = {
    MongoFactory.db(tableName)
  }
}
