package com.itcast.test

import com.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import com.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, KafkaProps}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/15 16:15
  */
object KafkaDataConsumerDemo {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val kafkaConsumer = new FlinkKafkaConsumer011[String](
      GlobalConfigUtil.output_topic_order,
      new SimpleStringSchema(),
      KafkaProps.getKafkaProperties()
    )
    val kafkaDataStream: DataStream[String] = env.addSource(kafkaConsumer)
    kafkaDataStream.print()

    env.execute()
  }

}
