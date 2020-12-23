package com.itcast.shop.realtime.etl.`trait`

import com.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
  * @ description: 定义特质，抽取所有etl操作公共的方法
  * @ author: spencer
  * @ date: 2020/12/10 13:39
  */
trait BaseETL[T] {

  /**
    * 创建kafka生产者对象
    * @param topic
    */
  def kafkaProducer(topic: String) = {
    // 将所有的ETL数据写入kafka，写入的时候都是json格式的数据
    new FlinkKafkaProducer011[String](
      topic,
      // 这种方式常用于读取kafka数据再写入kafka数据的场景
      new KeyedSerializationSchemaWrapper[String](new SimpleStringSchema()),
      KafkaProps.getKafkaProperties()
    )
  }

  /**
    * 根据业务抽取出来的kafka读取方法，因为所有的ETL都会操作kafka
    * @param topic
    * @return
    */
  def getKafkaDataStream(topic: String): DataStream[T]

  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    */
  def process()
}
