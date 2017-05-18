package com.palmap.rssi.common


import org.apache.curator.framework.{CuratorFrameworkFactory, CuratorFramework}
import org.apache.curator.retry.RetryNTimes

object WriteToZK {

  def main(args: Array[String]) {

    if (args.length < 2) {
      println("too few args!")
      System.exit(0)
    }

    val zNodePath = args(0)
    val data = args(1)
    val appConf = GeneralMethods.getConf(Common.SPARK_CONFIG)
    val zkQuorum = appConf(Common.ZOOKEEPER_QUORUM)
    val client: CuratorFramework = CuratorFrameworkFactory.newClient(zkQuorum, new RetryNTimes(1000, 3))
    client.start()
    client.setData().forPath(zNodePath, data.getBytes())

    if (client != null) {
      client.close()
    }

  }

}
