package com.itcast.shop.realtime.etl.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * @ description: 连接redis的工具类
  * @ author: spencer
  * @ date: 2020/12/10 11:17
  */
object RedisUtil {

  private val config = new JedisPoolConfig()

  // 是否启用后进先出，默认true
  config.setLifo(true)

  // 最大空闲连接数，默认8个
  config.setMaxIdle(8)
  // 最大连接数
  config.setMaxTotal(1000)
  // 获取连接时的最大等待时间，默认-1
  config.setMaxWaitMillis(-1)
  // 逐出连接的最小空闲时间，默认1800000(30min)
  config.setMinEvictableIdleTimeMillis(1800000)
  // 最小空闲连接数，默认0
  config.setMinIdle(0)
  // 每次逐出检查时，逐出的最大数目，默认3
  config.setNumTestsPerEvictionRun(3)
  // 对象空间多久后逐出
  config.setSoftMinEvictableIdleTimeMillis(1800000)
  // 在获取连接的时候检查有效性，默认false
  config.setTestOnBorrow(false)
  // 在空闲时检查有效性，默认false
  config.setTestWhileIdle(false)

  // 初始化redis连接池对象
  private val jedisPool = new JedisPool(config, GlobalConfigUtil.redis_server_ip)

  def getPool() = {
    jedisPool
  }

  def getJedis() = {
    val jedis: Jedis = jedisPool.getResource
    jedis.auth(GlobalConfigUtil.redis_server_password)
    jedis
  }
}
