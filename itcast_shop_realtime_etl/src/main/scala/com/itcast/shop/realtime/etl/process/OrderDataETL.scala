package com.itcast.shop.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import com.itcast.shop.realtime.etl.bean.OrderDBEntity
import com.itcast.shop.realtime.etl.utils.GlobalConfigUtil
import org.apache.flink.streaming.api.scala._

/**
  * @ description: 订单数据的实时ETL
  * @ author: spencer
  * @ date: 2020/12/15 15:42
  */
class OrderDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env){
  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    *
    */
  override def process(): Unit = {
    /**
      * 实现步骤：
      * 1.读取kafka中的数据源，过滤出订单数据
      * 2.解析rowData对象，转换成OrderDBEntity对象
      * 3.将OrderDBEntity对象转换成json字符串
      * 4.将json字符串写入kafka
      */

    // 1.读取kafka中的数据源，过滤出订单数据
    val orderDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "itcast_orders")

    // 2.解析rowData对象，转换成OrderDBEntity对象
    val orderEntityDataStream: DataStream[OrderDBEntity] = orderDataStream.map(rowData => {
      OrderDBEntity(rowData)
    })

    // 3.将OrderDBEntity对象转换成json字符串
    val orderJsonDataStream: DataStream[String] = orderEntityDataStream.map(orderEntityDataStream => {
      JSON.toJSONString(orderEntityDataStream, SerializerFeature.DisableCircularReferenceDetect)
    })

    orderJsonDataStream.print("订单数据：")
    // 4.将json字符串写入kafka
    orderJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.output_topic_order))
  }
}
