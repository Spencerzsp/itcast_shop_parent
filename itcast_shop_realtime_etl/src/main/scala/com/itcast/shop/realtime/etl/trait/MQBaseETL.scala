package com.itcast.shop.realtime.etl.`trait`

import com.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @ description: 根据数据的来源不同，可以抽取出来两个抽象类
  *                该类主要是消费日志数据和购物车以及评论数据，这类数据是字符串，直接使用string序列化
  * @ author: spencer
  * @ date: 2020/12/10 13:45
  */
abstract class MQBaseETL(env: StreamExecutionEnvironment) extends BaseETL[String]{
  /**
    * 根据业务抽取出来的kafka读取方法，因为所有的ETL都会操作kafka
    *
    * @param topic
    * @return
    */
  override def getKafkaDataStream(topic: String): DataStream[String] = {
    val kafkaProducer = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      KafkaProps.getKafkaProperties()
    )

    val logDataStream: DataStream[String] = env.addSource(kafkaProducer)

    logDataStream
  }

//  /**
//    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
//    *
//    * @param topic
//    */
//  override def process(topic: String): Unit = ???
}
