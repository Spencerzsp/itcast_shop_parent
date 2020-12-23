package com.itcast.shop.realtime.etl.utils

import com.typesafe.config.{Config, ConfigFactory}

/**
  * @ description: 编写读取配置文件的工具类
  * @ author: spencer
  * @ date: 2020/12/10 10:52
  */
object GlobalConfigUtil {



  // 默认在resource目录下找appliaciton名字的配置文件
  private val config: Config = ConfigFactory.load()

  val bootstrap_servers: String = config.getString("bootstrap.servers")
  val zookeeper_connect: String = config.getString("zookeeper.connect")
  val group_id: String = config.getString("group.id")
  val auto_commit_intervals_ms: String = config.getString("auto.commit.interval.ms")
  val enable_auto_commit: String = config.getString("enable.auto.commit")
  val auto_offset_reset: String = config.getString("auto.offset.reset")
  val key_serializer: String = config.getString("key.serializer")
  val key_deserializer: String = config.getString("key.deserializer")
  val output_topic_order: String = config.getString("output.topic.order")
  val output_topic_order_detail: String = config.getString("output.topic.order_detail")
  val output_topic_clicklog: String = config.getString("output.topic.clicklog")
  val output_topic_goods: String = config.getString("output.topic.goods")
  val output_topic_cart: String = config.getString("output.topic.cart")
  val output_topic_ordertimeout: String = config.getString("output.topic.ordertimeout")
  val output_topic_comments: String = config.getString("output.topic.comments")
  val hbase_table_orderdetail: String = config.getString("hbase.table.orderdetail")
  val hbase_table_family: String = config.getString("hbase.table.family")
  val hbase_table_goods: String = config.getString("hbase.table.goods_wide")
  val hbase_table_goods_family: String = config.getString("hbase.table.goods_family")
  val redis_server_ip: String = config.getString("redis.server.ip")
  val redis_server_port: String = config.getString("redis.server.port")
  val redis_server_password: String = config.getString("redis.server.password")
  val ip_file_path: String = config.getString("ip.flile.path")
  val mysql_server_ip: String = config.getString("mysql.server.ip")
  val mysql_server_port: String = config.getString("mysql.server.port")
  val mysql_server_database: String = config.getString("mysql.server.database")
  val mysql_server_username: String = config.getString("mysql.server.username")
  val mysql_server_password: String = config.getString("mysql.server.password")
  val input_topic_cart: String = config.getString("input.topic.cart")
  val input_topic_canal: String = config.getString("input.topic.canal")
  val input_topic_click_log: String = config.getString("input.topic.click_log")
  val input_topic_comments: String = config.getString("input.topic.comments")

  def main(args: Array[String]): Unit = {
    println(input_topic_cart)
    println(config.getString("hbase.table.goods_wide"))
    println(auto_offset_reset)
  }
}
