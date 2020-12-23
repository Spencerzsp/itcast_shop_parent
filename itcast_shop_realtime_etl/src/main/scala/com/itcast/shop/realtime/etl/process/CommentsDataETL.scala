package com.itcast.shop.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.shop.realtime.etl.`trait`.MQBaseETL
import com.itcast.shop.realtime.etl.bean.{CommentsEntity, CommentsWideEntity, DimGoodsDBEntity}
import com.itcast.shop.realtime.etl.utils.{DateUtil, GlobalConfigUtil, RedisUtil}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

/**
  * @ description:
  * @ author: spencer
  * @ date: 2020/12/17 15:37
  */
class CommentsDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env){
  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    */
  override def process(): Unit = {
    /**
      * 实现步骤：
      * 1.获取kafka数据源，将kafka中的字符串数据转换成评论数据的实体类
      * 2.将转换后的实体类进行实时拉宽
      * 3.将拉宽后的实体类转换成json字符串写入kafka
      *
      */
    // 1.获取kafka数据源，将kafka中的字符串数据转换成评论数据的实体类
    val commentsJsonDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.input_topic_comments)
    val commentsEntityDataStream: DataStream[CommentsEntity] = commentsJsonDataStream.map(commentsJson => {
      CommentsEntity(commentsJson)
    })

    // 2.将转换后的实体类进行实时拉宽
    val commentsWideDataStream: DataStream[CommentsWideEntity] = commentsEntityDataStream.map(new RichMapFunction[CommentsEntity, CommentsWideEntity] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      override def close(): Unit = {
        if (jedis.isConnected) {
          jedis.close()
        }
      }

      // 处理数据
      override def map(commentsEntity: CommentsEntity): CommentsWideEntity = {
        // 获取默认的参数
        val commentsWideEntity = CommentsWideEntity(commentsEntity)

        // 关联redis查询拉宽后的数据
        val goodsJson: String = jedis.hget("itcast_shop:dim_goods", commentsEntity.goodsId)
        val dimGoods = DimGoodsDBEntity(goodsJson)

        commentsWideEntity.goodsName = dimGoods.goodsName
        commentsWideEntity.shopId = dimGoods.shopId

        // 将时间戳转换为指定格式的时间类型
        commentsWideEntity.createTime = DateUtil.timestampSeconds2Str(commentsEntity.timestamp, "yyyy-MM-dd HH:mm:ss")

        commentsWideEntity
      }
    })

    commentsWideDataStream.print("拉宽后的评论数据：")

    // 3.将拉宽后的实体类转换成json字符串写入kafka
    val commentsWideJson: DataStream[String] = commentsWideDataStream.map(commentsWide => {
      JSON.toJSONString(commentsWide, SerializerFeature.DisableCircularReferenceDetect)
    })
    commentsWideJson.addSink(kafkaProducer(GlobalConfigUtil.output_topic_comments))
  }
}
