package com.itcast.shop.realtime.etl.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

/**
  * @ description: 定义kafka的属性配置类
  * @ author: spencer
  * @ date: 2020/12/10 13:51
  */
object KafkaProps {

  /**
    * 获取kafka的配置信息
    */
  def getKafkaProperties() = {
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfigUtil.bootstrap_servers)
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GlobalConfigUtil.group_id)
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, GlobalConfigUtil.enable_auto_commit)
    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, GlobalConfigUtil.auto_commit_intervals_ms)
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, GlobalConfigUtil.auto_offset_reset)
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, GlobalConfigUtil.key_deserializer)

    // 将封装后的kafka配置项返回
    props
  }

  /**
    * 获取kafka生产者的配置信息
    */
  def getKafkaProducerProperties() = {
    val props = new Properties()
    props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, GlobalConfigUtil.bootstrap_servers)
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, GlobalConfigUtil.key_serializer)
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GlobalConfigUtil.key_serializer)

    props
  }

  def main(args: Array[String]): Unit = {

  }
}
