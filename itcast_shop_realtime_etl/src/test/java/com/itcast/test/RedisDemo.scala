package com.itcast.test

import com.itcast.shop.realtime.etl.utils.RedisUtil
import redis.clients.jedis.Jedis

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/16 10:00
  */
object RedisDemo {

  def main(args: Array[String]): Unit = {
    val jedis: Jedis = RedisUtil.getJedis()
    jedis.select(1)

    val json: String = jedis.hget("itcast_shop:dim_goods", "115019")
    println(json)
  }
}
