package com.bigdata.itcast.hotitems

import com.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.Trigger
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/28 17:31
  */
object UVCountAnalysis {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1.获取kafka数据源
    val rawDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](
      "user_behavior",
      new SimpleStringSchema(),
      KafkaProps.getKafkaProperties()
    ))

    // 2.将kafka中的数据转换为样例类所对应的DataStream，并设置watermark
    val itemViewDataStream: DataStream[ItemView] = rawDataStream.map(value => {
      val dataArray: Array[String] = value.split(",")
      ItemView(
        dataArray(0).toLong,
        dataArray(1).toLong,
        dataArray(2).toInt,
        dataArray(3),
        dataArray(4).toLong
      )
    })
      //      .assignAscendingTimestamps(_.timestamp)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ItemView](Time.seconds(10)) {
      override def extractTimestamp(element: ItemView): Long = element.timestamp
    })

    // 3.过滤出pv的数据
    val itemViewPVDataStream: DataStream[ItemView] = itemViewDataStream.filter(_.behavior == "pv")

//    // 4.按照userId进行分组
//    itemViewPVDataStream.map(value => ("dumkey", value.userId))
//      .keyBy(_._1)
//      .timeWindow(Time.minutes(1), Time.seconds(20))
//      .trigger(new Trigger[ItemView, TimeWindow] {})

  }
}
