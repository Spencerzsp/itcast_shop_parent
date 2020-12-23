package com.itcast.shop.realtime.etl.process

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import com.itcast.shop.realtime.etl.async.AsyncOrderDetailRedisRequest
import com.itcast.shop.realtime.etl.bean.OrderGoodsWideDBEntity
import com.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, HBaseUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  * @ description: 1）将订单明细数据的事实表和维度表进行实时拉宽，写入habse
  *                2）将拉宽后的数据写入kafka，供druid摄取
  * @ author: spencer
  * @ date: 2020/12/15 16:44
  */
class OderDetailDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env){
  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    */
  override def process(): Unit = {
    /**
      * 实现步骤：
      * 1.获取canal中的数据，过滤出订单明细数据，将canalRowdata转换成OrderGoods样例类
      * 2.将订单明细表的数据进行实时拉宽
      * 3.将拉宽后的数据转换成json字符串，写入kafka供druid摄取
      * 4.将拉宽后的数据转换成json字符串，写入hbase供后续订单明细数据的查询
      */

    // 1.获取canal中的数据，过滤出订单明细数据，将canalRowdata转换成OrderGoods样例类
    val orderGoodsCanalDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "itcast_order_goods")

    // 2.将订单明细表的数据进行实时拉宽（使用异步io）
    val orderGoodsWideDataStream: DataStream[OrderGoodsWideDBEntity] = AsyncDataStream.unorderedWait(orderGoodsCanalDataStream, new AsyncOrderDetailRedisRequest(), 1, TimeUnit.MINUTES, 100)


    // 3.将拉宽后的数据转换成json字符串，写入kafka供druid摄取
    val orderGoodsWideJson: DataStream[String] = orderGoodsWideDataStream.map(orderGoodsWideDBEntity => {
      JSON.toJSONString(orderGoodsWideDBEntity, SerializerFeature.DisableCircularReferenceDetect)
    })
    orderGoodsWideDataStream.print("拉宽后的订单明细数据：")
    orderGoodsWideJson.addSink(kafkaProducer(GlobalConfigUtil.output_topic_order_detail))

    // 4.将拉宽后的数据写入hbase
    orderGoodsWideDataStream.addSink(new RichSinkFunction[OrderGoodsWideDBEntity] {
      // 定义hbase的连接对象
      var connection: Connection = _
      // 定义hbase的表
      var table: Table = _
      override def open(parameters: Configuration): Unit = {
        connection = HBaseUtil.getPool().getConnection()
        table = connection.getTable(TableName.valueOf(GlobalConfigUtil.hbase_table_orderdetail))
      }

      override def close(): Unit = {
        if (table != null) table.close()
        if (!connection.isClosed) HBaseUtil.getPool().returnConnection(connection)
      }

      /**
        * 将数据写入hbase
        * @param orderGoodsWideDBEntity
        * @param context
        */
      override def invoke(orderGoodsWideDBEntity: OrderGoodsWideDBEntity, context: SinkFunction.Context[_]): Unit = {
        // 构建put对象
        // 使用订商品id和订单明细id作为rowKey
        val rowKey: Array[Byte] = Bytes.toBytes(orderGoodsWideDBEntity.goodsId + "_" + orderGoodsWideDBEntity.ogId)
        val put = new Put(rowKey)

        // 创建column family
        val family: Array[Byte] = Bytes.toBytes(GlobalConfigUtil.hbase_table_family)

        // 创建列
        val ogId: Array[Byte] = Bytes.toBytes("ogId")
        val orderId: Array[Byte] = Bytes.toBytes("orderId")
        val goodsId: Array[Byte] = Bytes.toBytes("goodsId")
        val goodsNum: Array[Byte] = Bytes.toBytes("goodsNum")
        val goodsPrice: Array[Byte] = Bytes.toBytes("goodsPrice")
        val goodsName: Array[Byte] = Bytes.toBytes("goodsName")
        val shopId: Array[Byte] = Bytes.toBytes("shopId")
        val goodsThirdCatId: Array[Byte] = Bytes.toBytes("goodsThirdCatId")
        val goodsThirdCatName: Array[Byte] = Bytes.toBytes("goodsThirdCatName")
        val goodsSecondCatId: Array[Byte] = Bytes.toBytes("goodsSecondCatId")
        val goodsSecondCatName: Array[Byte] = Bytes.toBytes("goodsSecondCatName")
        val goodsFirstCatId: Array[Byte] = Bytes.toBytes("goodsFirstCatId")
        val goodsFirstCatName: Array[Byte] = Bytes.toBytes("goodsFirstCatName")
        val areaId: Array[Byte] = Bytes.toBytes("areaId")
        val shopName: Array[Byte] = Bytes.toBytes("shopName")
        val shopCompany: Array[Byte] = Bytes.toBytes("shopCompany")
        val cityId: Array[Byte] = Bytes.toBytes("cityId")
        val cityName: Array[Byte] = Bytes.toBytes("cityName")
        val regionId: Array[Byte] = Bytes.toBytes("regionId")
        val regionName: Array[Byte] = Bytes.toBytes("regionName")

        put.addColumn(family, ogId, Bytes.toBytes(orderGoodsWideDBEntity.ogId.toString))
        put.addColumn(family, orderId, Bytes.toBytes(orderGoodsWideDBEntity.orderId.toString))
        put.addColumn(family, goodsId, Bytes.toBytes(orderGoodsWideDBEntity.goodsId.toString))
        put.addColumn(family, goodsNum, Bytes.toBytes(orderGoodsWideDBEntity.goodsNum.toString))
        put.addColumn(family, goodsPrice, Bytes.toBytes(orderGoodsWideDBEntity.goodsPrice.toString))
        put.addColumn(family, goodsName, Bytes.toBytes(orderGoodsWideDBEntity.goodsName))
        put.addColumn(family, shopId, Bytes.toBytes(orderGoodsWideDBEntity.shopId.toString))
        put.addColumn(family, goodsThirdCatId, Bytes.toBytes(orderGoodsWideDBEntity.goodsThirdCatId.toString))
        put.addColumn(family, goodsThirdCatName, Bytes.toBytes(orderGoodsWideDBEntity.goodsThirdCatName))
        put.addColumn(family, goodsSecondCatId, Bytes.toBytes(orderGoodsWideDBEntity.goodsSecondCatId.toString))
        put.addColumn(family, goodsSecondCatName, Bytes.toBytes(orderGoodsWideDBEntity.goodsSecondCatName))
        put.addColumn(family, goodsFirstCatId, Bytes.toBytes(orderGoodsWideDBEntity.goodsFirstCatId.toString))
        put.addColumn(family, goodsFirstCatName, Bytes.toBytes(orderGoodsWideDBEntity.goodsFirstCatName))
        put.addColumn(family, areaId, Bytes.toBytes(orderGoodsWideDBEntity.areaId.toString))
        put.addColumn(family, shopName, Bytes.toBytes(orderGoodsWideDBEntity.shopName))
        put.addColumn(family, shopCompany, Bytes.toBytes(orderGoodsWideDBEntity.shopCompany))
        put.addColumn(family, cityId, Bytes.toBytes(orderGoodsWideDBEntity.cityId.toString))
        put.addColumn(family, cityName, Bytes.toBytes(orderGoodsWideDBEntity.cityName))
        put.addColumn(family, regionId, Bytes.toBytes(orderGoodsWideDBEntity.regionId.toString))
        put.addColumn(family, regionName, Bytes.toBytes(orderGoodsWideDBEntity.regionName))

        // 执行put操作
        table.put(put)
      }
    })

  }
}
