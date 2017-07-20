package com.palmap.rssi.common

import scala.collection.mutable

import org.apache.curator.framework.recipes.cache.{NodeCache, NodeCacheListener}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.utils.EnsurePath


/**
  * zookeeper相关类 被触发后跟新部分内存数据
  */
object ZKMonitor {

  val xmlConf: mutable.Map[String, String] = GeneralMethods.getConf(Common.SPARK_CONFIG)
  val zkMapMonitorPath: String = xmlConf(Common.ZK_MAP_MONITOR_PATH)
  val sceneIdPath: String = xmlConf(Common.SCENE_ID_MONITOR_PATH)

  def startMonitor(): Unit = {

    val retryPolicy = new ExponentialBackoffRetry(1000,3)
    val client: CuratorFramework = CuratorFrameworkFactory.newClient(xmlConf(Common.ZOOKEEPER_QUORUM), retryPolicy)
    client.start()

    val confEnsurePath:EnsurePath = client.newNamespaceAwareEnsurePath(zkMapMonitorPath)
    confEnsurePath.ensure(client.getZookeeperClient)

    val nodeMonitor = confNodeCache(client, zkMapMonitorPath)
    val mapNodeMonitor = MapNodeCache(client,zkMapMonitorPath)

    nodeMonitor.start(true)
    mapNodeMonitor.start(true)

    println(zkMapMonitorPath + " start zk monitor....")
  }

  /**
    * to be removed
    */
  def updateSceneIdNodeCache(client: CuratorFramework, path: String): NodeCache = {

    val cache: NodeCache  = new NodeCache (client, path)

    cache.getListenable.addListener(new NodeCacheListener {

      override def nodeChanged(): Unit = {

        val zNodeData = new String(cache.getCurrentData.getData)

        try{
          CommonConf.sceneIdlist += zNodeData.toInt
        }catch {
          case _: Exception => println("bad sceneId zkNode data: " + zNodeData)
        }
      }
    })

    cache
  }

  /**
    * 接收新场景 添加至内存
    * @param client
    * @param path
    * @return
    */
  def MapNodeCache(client: CuratorFramework, path: String): NodeCache = {

    val cache: NodeCache  = new NodeCache (client, path)

    cache.getListenable.addListener(new NodeCacheListener {

      override def nodeChanged(): Unit = {

        val zNodeData = new String(cache.getCurrentData.getData)

        try {
          val info = zNodeData.split(",", -1)
          CommonConf.sceneIdlist.add(info(0).toInt)
          CommonConf.sceneIdMap.put(info(0).toInt,info(1).toInt)
        } catch {
          case _: Exception => println("bad zkNode data: " + zNodeData)
        }
      }
    })

    cache
  }

  /**
    * 接收营业时间 更新至内存
    * @param client
    * @param path
    * @return
    */
  def confNodeCache(client: CuratorFramework, path: String): NodeCache = {

    val cache: NodeCache  = new NodeCache (client, path)

    cache.getListenable.addListener(new NodeCacheListener {

      override def nodeChanged(): Unit = {

        val zNodeData = new String(cache.getCurrentData.getData)
        println("confNodeCache changed, data is: " + zNodeData)

        try {

          ConfInfoSet.initSceneIdList()

          val info = zNodeData.split(Common.CTRL_A, -1)

          info(0) match {
            case Common.STORE_BUSINESS_HOURS =>
              val sceneId =  info(1).toInt
              ConfInfoSet.updateBusinessHourMap(sceneId)
            case  Common.ZK_MACHINE_SET => ConfInfoSet.updateMachineSet() //update machine set
            case _ => println(info(0))
          }
        } catch {
          case _: Exception => println("bad zkNode data: " + zNodeData)
        }
      }
    })

    cache
  }

}
