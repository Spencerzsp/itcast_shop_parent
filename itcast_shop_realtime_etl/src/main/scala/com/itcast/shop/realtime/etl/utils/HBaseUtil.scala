package com.itcast.shop.realtime.etl.utils

import com.itcast.shop.realtime.etl.utils.pool.{ConnectionPoolConfig, HBaseConnectionPool}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration


/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/10 11:39
  */
object HBaseUtil {

  private val config = new ConnectionPoolConfig
  config.setMaxIdle(20)
  config.setMaxIdle(5)
  config.setMaxWaitMillis(1000)
  config.setTestOnBorrow(true)

  var hbaseConfig: Configuration = HBaseConfiguration.create()
  hbaseConfig = HBaseConfiguration.create
  hbaseConfig.set("hbase.default.for.version.skip", "true")

  // 创建连接池对象
  private val pool = new HBaseConnectionPool(config, hbaseConfig)

  def getPool() = {
    pool
  }

}
