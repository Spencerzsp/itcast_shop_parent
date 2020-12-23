package com.itcast.shop.realtime.etl.`trait`

import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.utils.{CanalRowDataDeserializerSchema, GlobalConfigUtil, KafkaProps}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @ description: 根据数据的来源不同，可以抽取出来两个抽象类
  *                该类主要是消费canalTopic中的数据，需要将消费到的字节码数据反序列化成canalRowData
  * @ author: spencer
  * @ date: 2020/12/10 13:43
  */
abstract class MysqlBaseETL(env: StreamExecutionEnvironment) extends BaseETL[CanalRowData]{
  /**
    * 根据业务抽取出来的kafka读取方法，因为所有的ETL都会操作kafka
    *
    * @param topic
    * @return
    */
  override def getKafkaDataStream(topic: String = GlobalConfigUtil.input_topic_canal): DataStream[CanalRowData] = {
    val canalKafkaConsumer = new FlinkKafkaConsumer011[CanalRowData](
      topic,
      new CanalRowDataDeserializerSchema(),
      KafkaProps.getKafkaProperties()
    )
    val canalDataStream: DataStream[CanalRowData] = env.addSource(canalKafkaConsumer)
    canalDataStream
  }

}
