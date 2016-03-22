package com.palmap.rssi.common

import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.EnsurePath

/**
 * Created by lingling.dai on 2015/12/15.
 */
object ZKMonitor {
  val xmlConf = GeneralMethods.getConf(Common.SPARK_CONFIG)
  val ZK_CONF_MONITOR_PATH="/nova/conf"
  def startMonitor()={
    val retryPolicy=new ExponentialBackoffRetry(1000,3)

    val client:CuratorFramework=CuratorFrameworkFactory.newClient(xmlConf("zkQuorum"),retryPolicy)
    client.start()
    val   confEnsurePath:EnsurePath=client.newNamespaceAwareEnsurePath(ZK_CONF_MONITOR_PATH)
    confEnsurePath.ensure(client.getZookeeperClient)

   val nodeMonitor=confNodeCache(client,ZK_CONF_MONITOR_PATH)
    nodeMonitor.start(true)
    println("start zk monitor....")


  }

  def confNodeCache(client: CuratorFramework, path: String): NodeCache = {
    val cache: NodeCache  = new NodeCache (client, path)
    cache.getListenable.addListener(new NodeCacheListener {
      override def nodeChanged(): Unit = {
        val znodeData = new String(cache.getCurrentData.getData)
        println("confNodeCache changed, data is: " + znodeData)
        try {
         // ConfInfoSet.getSceneIdlist()

          val info = znodeData.split(Common.CTRL_A, -1)
          info(0) match {
            case Common.STORE_BUSINESS_HOURS => {
              val sceneId =  info(1).toInt
              ConfInfoSet.updateBusinessHourMap(sceneId)
            }
            case _ => println(info(0))
          }
        } catch {
          case e: Exception => println("bad zkNode data: " + znodeData)
        }
      }
    })
    cache
  }


}
