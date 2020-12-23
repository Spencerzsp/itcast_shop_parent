package com.itcast.shop.realtime.etl.process

import java.util.concurrent.TimeUnit

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import com.itcast.shop.realtime.etl.async.AsyncGoodsRedisRequest
import com.itcast.shop.realtime.etl.bean.GoodsWideEntity
import com.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, HBaseUtil}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes

/**
  * @ description:
  * @ author: spencer
  * @ date: 2020/12/16 16:12
  */
class GoodsDataETL(env: StreamExecutionEnvironment) extends MysqlBaseETL(env){
  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    */
  override def process(): Unit = {
    val goodsCanalDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "itcast_goods")
//    goodsCanalDataStream.print("itcast_goods:rowData")
    val goodsWideDataStream: DataStream[GoodsWideEntity] = AsyncDataStream.unorderedWait(goodsCanalDataStream, new AsyncGoodsRedisRequest(), 1, TimeUnit.MINUTES, 100)

    goodsWideDataStream.print("拉宽后的商品数据：")

    // 将拉宽后的数据转换成json字符串，写入kafka供druid摄取
    val goodsWideJson: DataStream[String] = goodsWideDataStream.map(goodsWideEntity => {
      JSON.toJSONString(goodsWideEntity, SerializerFeature.DisableCircularReferenceDetect)
    })
    goodsWideJson.addSink(kafkaProducer(GlobalConfigUtil.output_topic_goods))

    // 将拉宽后的数据写入hbase
    goodsWideDataStream.addSink(new RichSinkFunction[GoodsWideEntity] {
      // 定义hbase连接对象
      var connection: Connection = _
      var table: Table = _
      override def open(parameters: Configuration): Unit = {
        connection  = HBaseUtil.getPool().getConnection()
        table = connection.getTable(TableName.valueOf(GlobalConfigUtil.hbase_table_goods))
      }

      override def invoke(goodsWideEntity: GoodsWideEntity, context: SinkFunction.Context[_]): Unit = {
        // 构建put对象
        val rowKey: String = goodsWideEntity.goodsId + "_" + goodsWideEntity.createTime
        val put = new Put(Bytes.toBytes(rowKey))

        // 创建列
        val goodsId: Array[Byte] = Bytes.toBytes("goodsId")
        val goodsSn: Array[Byte] = Bytes.toBytes("goodsSn")
        val productNo: Array[Byte] = Bytes.toBytes("productNo")
        val goodsName: Array[Byte] = Bytes.toBytes("goodsName")
        val goodsImg: Array[Byte] = Bytes.toBytes("goodsImg")
        val shopId: Array[Byte] = Bytes.toBytes("shopId")
        val shopName: Array[Byte] = Bytes.toBytes("shopName")
        val goodsType: Array[Byte] = Bytes.toBytes("goodsType")
        val marketPrice: Array[Byte] = Bytes.toBytes("marketPrice")
        val shopPrice: Array[Byte] = Bytes.toBytes("shopPrice")
        val warnStock: Array[Byte] = Bytes.toBytes("warnStock")
        val goodsStock: Array[Byte] = Bytes.toBytes("goodsStock")
        val goodsTips: Array[Byte] = Bytes.toBytes("goodsTips")
        val isSale: Array[Byte] = Bytes.toBytes("isSale")
        val isBest: Array[Byte] = Bytes.toBytes("isBest")
        val isHot: Array[Byte] = Bytes.toBytes("isHot")
        val isNew: Array[Byte] = Bytes.toBytes("isNew")
        val isRecom: Array[Byte] = Bytes.toBytes("isRecom")
        val goodsCatIdPath: Array[Byte] = Bytes.toBytes("goodsCatIdPath")
        val goodsThirdCatId: Array[Byte] = Bytes.toBytes("goodsThirdCatId")
        val goodsThirdCatName: Array[Byte] = Bytes.toBytes("goodsThirdCatName")
        val goodsSecondCatId: Array[Byte] = Bytes.toBytes("goodsSecondCatId")
        val goodsSecondCatName: Array[Byte] = Bytes.toBytes("goodsSecondCatName")
        val goodsFirstCatId: Array[Byte] = Bytes.toBytes("goodsFirstCatId")
        val goodsFirstCatName: Array[Byte] = Bytes.toBytes("goodsFirstCatName")
        val shopCatId1: Array[Byte] = Bytes.toBytes("shopCatId1")
        val shopCatName1: Array[Byte] = Bytes.toBytes("shopCatName1")
        val shopCatId2: Array[Byte] = Bytes.toBytes("shopCatId2")
        val shopCatName2: Array[Byte] = Bytes.toBytes("shopCatName2")
        val brandId: Array[Byte] = Bytes.toBytes("brandId")
        val goodsDesc: Array[Byte] = Bytes.toBytes("goodsDesc")
        val goodsStatus: Array[Byte] = Bytes.toBytes("goodsStatus")
        val saleNum: Array[Byte] = Bytes.toBytes("saleNum")
        val saleTime: Array[Byte] = Bytes.toBytes("saleTime")
        val visitNum: Array[Byte] = Bytes.toBytes("visitNum")
        val appraiseNum: Array[Byte] = Bytes.toBytes("appraiseNum")
        val isSpec: Array[Byte] = Bytes.toBytes("isSpec")
        val gallery: Array[Byte] = Bytes.toBytes("gallery")
        val goodsSeoKeywords: Array[Byte] = Bytes.toBytes("goodsSeoKeywords")
        val illegalRemarks: Array[Byte] = Bytes.toBytes("illegalRemarks")
        val dataFlag: Array[Byte] = Bytes.toBytes("dataFlag")
        val createTime: Array[Byte] = Bytes.toBytes("createTime")
        val isFreeShipping: Array[Byte] = Bytes.toBytes("isFreeShipping")
        val goodsSearchKeywords: Array[Byte] = Bytes.toBytes("goodsSearchKeywords")
        val modifyTime: Array[Byte] = Bytes.toBytes("modifyTime")
        val cityId: Array[Byte] = Bytes.toBytes("cityId")
        val cityName: Array[Byte] = Bytes.toBytes("cityName")
        val regionId: Array[Byte] = Bytes.toBytes("regionId")
        val regionName: Array[Byte] = Bytes.toBytes("regionName")

        // 封装列
        val family: Array[Byte] = Bytes.toBytes(GlobalConfigUtil.hbase_table_goods_family)
        put.addColumn(family, goodsId, Bytes.toBytes(goodsWideEntity.goodsId.toString))
        put.addColumn(family, goodsSn, Bytes.toBytes(goodsWideEntity.goodsSn))
        put.addColumn(family, productNo, Bytes.toBytes(goodsWideEntity.productNo))
        put.addColumn(family, goodsName, Bytes.toBytes(goodsWideEntity.goodsName))
        put.addColumn(family, goodsImg, Bytes.toBytes(goodsWideEntity.goodsImg))
        put.addColumn(family, shopId, Bytes.toBytes(goodsWideEntity.shopId))
        put.addColumn(family, shopName, Bytes.toBytes(goodsWideEntity.shopName))
        put.addColumn(family, goodsType, Bytes.toBytes(goodsWideEntity.goodsType))
        put.addColumn(family, marketPrice, Bytes.toBytes(goodsWideEntity.marketPrice))
        put.addColumn(family, shopPrice, Bytes.toBytes(goodsWideEntity.shopPrice))
        put.addColumn(family, warnStock, Bytes.toBytes(goodsWideEntity.warnStock))
        put.addColumn(family, goodsStock, Bytes.toBytes(goodsWideEntity.goodsStock))
        put.addColumn(family, goodsTips, Bytes.toBytes(goodsWideEntity.goodsTips))
        put.addColumn(family, isSale, Bytes.toBytes(goodsWideEntity.isSale))
        put.addColumn(family, isBest, Bytes.toBytes(goodsWideEntity.isBest))
        put.addColumn(family, isHot, Bytes.toBytes(goodsWideEntity.isHot))
        put.addColumn(family, isNew, Bytes.toBytes(goodsWideEntity.isNew))
        put.addColumn(family, isRecom, Bytes.toBytes(goodsWideEntity.isRecom))
        put.addColumn(family, goodsCatIdPath, Bytes.toBytes(goodsWideEntity.goodsCatIdPath))
        put.addColumn(family, goodsThirdCatId, Bytes.toBytes(goodsWideEntity.goodsThirdCatId.toString))
        put.addColumn(family, goodsThirdCatName, Bytes.toBytes(goodsWideEntity.goodsThirdCatName))
        put.addColumn(family, goodsSecondCatId, Bytes.toBytes(goodsWideEntity.goodsSecondCatId.toString))
        put.addColumn(family, goodsSecondCatName, Bytes.toBytes(goodsWideEntity.goodsSecondCatName))
        put.addColumn(family, goodsFirstCatId, Bytes.toBytes(goodsWideEntity.goodsFirstCatId.toString))
        put.addColumn(family, goodsFirstCatName, Bytes.toBytes(goodsWideEntity.goodsFirstCatName))
        put.addColumn(family, shopCatId1, Bytes.toBytes(goodsWideEntity.shopCatId1.toString))
        put.addColumn(family, shopCatName1, Bytes.toBytes(goodsWideEntity.shopCatName1))
        put.addColumn(family, shopCatId2, Bytes.toBytes(goodsWideEntity.shopCatId2.toString))
        put.addColumn(family, shopCatName2, Bytes.toBytes(goodsWideEntity.shopCatName2))
        put.addColumn(family, brandId, Bytes.toBytes(goodsWideEntity.brandId))
        put.addColumn(family, goodsDesc, Bytes.toBytes(goodsWideEntity.goodsDesc))
        put.addColumn(family, goodsStatus, Bytes.toBytes(goodsWideEntity.goodsStatus))
        put.addColumn(family, saleNum, Bytes.toBytes(goodsWideEntity.saleNum))
        put.addColumn(family, saleTime, Bytes.toBytes(goodsWideEntity.saleTime))
        put.addColumn(family, visitNum, Bytes.toBytes(goodsWideEntity.visitNum))
        put.addColumn(family, appraiseNum, Bytes.toBytes(goodsWideEntity.appraiseNum))
        put.addColumn(family, isSpec, Bytes.toBytes(goodsWideEntity.isSpec))
        put.addColumn(family, gallery, Bytes.toBytes(goodsWideEntity.gallery))
        put.addColumn(family, goodsSeoKeywords, Bytes.toBytes(goodsWideEntity.goodsSeoKeywords))
        put.addColumn(family, illegalRemarks, Bytes.toBytes(goodsWideEntity.illegalRemarks))
        put.addColumn(family, dataFlag, Bytes.toBytes(goodsWideEntity.dataFlag))
        put.addColumn(family, createTime, Bytes.toBytes(goodsWideEntity.createTime))
        put.addColumn(family, isFreeShipping, Bytes.toBytes(goodsWideEntity.isFreeShipping))
        put.addColumn(family, goodsSearchKeywords, Bytes.toBytes(goodsWideEntity.goodsSearchKeywords))
        put.addColumn(family, modifyTime, Bytes.toBytes(goodsWideEntity.modifyTime))
        put.addColumn(family, cityId, Bytes.toBytes(goodsWideEntity.cityId.toString))
        put.addColumn(family, cityName, Bytes.toBytes(goodsWideEntity.cityName))
        put.addColumn(family, regionId, Bytes.toBytes(goodsWideEntity.regionId.toString))
        put.addColumn(family, regionName, Bytes.toBytes(goodsWideEntity.regionName))

        // 执行put操作，写入hbase
        table.put(put)

      }

      override def close(): Unit = {
        if (table != null) table.close()
        if (!connection.isClosed) HBaseUtil.getPool().returnConnection(connection)
      }
    })
  }
}
