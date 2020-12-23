package com.itcast.shop.realtime.etl.utils.pool

import java.util.concurrent.{ExecutorService, Executors}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}

/**
  * @ description:
  * @ author: spencer
  * @ date: 2020/12/10 13:09
  */
class HBaseConnectionPool(config: ConnectionPoolConfig, hbaseConfig: Configuration){

  private val executor: ExecutorService = Executors.newFixedThreadPool(20)
  /**
    * 获取连接池对象
    * @return
    */
  def getConnection() = {
    val connection: Connection = ConnectionFactory.createConnection(hbaseConfig, executor)
    connection
  }

  /**
    * 将对象还回连接池
    */
  def returnConnection(connection: Connection) = {
    executor.shutdown()
  }

}
