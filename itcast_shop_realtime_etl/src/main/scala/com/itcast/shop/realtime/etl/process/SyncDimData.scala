package com.itcast.shop.realtime.etl.process

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import com.itcast.shop.realtime.etl.bean._
import com.itcast.shop.realtime.etl.utils.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

/**
  * @ description: 增量更新维度表数据到redis中(5张表)
  * @ author: spencer
  * @ date: 2020/12/11 11:00
  */
case class SyncDimData(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    *
    */
  override def process(): Unit = {
    /**
      * 实现步骤
      * 1：获取数据源
      * 2：过滤出维度数据
      * 3：处理同步过来的数据，更新到redis
      */
    // 1.获取数据源
    val canalDataStream: DataStream[CanalRowData] = getKafkaDataStream()

//    canalDataStream.print()
    // 2.过滤维度数据(当维度表中的数据变化时，使用canal同步到kafka中)
    val dimRowDataStream: DataStream[CanalRowData] = canalDataStream.filter(
      rowData => {
        rowData.getTableName match {
          case "itcast_goods" => true
          case "itcast_shops" => true
          case "itcast_goods_cats" => true
          case "itcast_org" => true
          case "itcast_shop_cats" => true
          case _ => false
        }
      }
    )

    // 3：处理同步过来的数据，更新到redis
    dimRowDataStream.addSink(new RichSinkFunction[CanalRowData] {
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      /**
        * 增量更新维度表数据
        *
        * @param rowData
        */
      def updateDimData(rowData: CanalRowData): Unit = {
        // 区分出来是操作的哪张维度表
        val tableName: String = rowData.getTableName
        tableName match {
          case "itcast_goods" => {
            val goodsId: Long = rowData.getColumns.get("goodsId").toLong
            val goodsName: String = rowData.getColumns.get("goodsName")
            val shopId: Long = rowData.getColumns.get("shopId").toLong
            val goodsCatId: Int = rowData.getColumns.get("goodsCatId").toInt
            val shopPrice: Double = rowData.getColumns.get("shopPrice").toDouble

            val dimGoodsDBEntity = DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId, shopPrice)

//            println(dimGoodsDBEntity)
            // 将对象转换为json字符串
            // SerializerFeature.DisableCircularReferenceDetect:防止循环引用抛出异常
            val entity: String = JSON.toJSONString(dimGoodsDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            println(entity)
            // 将数据写入redis
            jedis.hset("itcast_shop:dim_goods", goodsId.toString, entity)
          }
          case "itcast_shops" => {
            val shopId: Int = rowData.getColumns.get("shopId").toInt
            val areaId: Int = rowData.getColumns.get("areaId").toInt
            val shopName: String = rowData.getColumns.get("shopName")
            val shopCompany: String = rowData.getColumns.get("shopCompany")

            val dimShopDBEntity = DimShopDBEntity(shopId, areaId, shopName, shopCompany)
            val entity: String = JSON.toJSONString(dimShopDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            println(entity)
            jedis.hset("itcast_shop:dim_shops", shopId.toString, entity)
          }
          case "itcast_goods_cats" => {
            val catId: String = rowData.getColumns.get("catId")
            val parentId: String = rowData.getColumns.get("parentId")
            val catName: String = rowData.getColumns.get("catName")
            val cat_level: String = rowData.getColumns.get("cat_level")

            val dimGoodsCatDBEntity = DimGoodsCatDBEntity(catId, parentId, catName, cat_level)
            val entity: String = JSON.toJSONString(dimGoodsCatDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            println(entity)

            jedis.hset("itcast_shop:dim_goods_cats", catId, entity)
          }
          case "itcast_org" => {
            val orgId: Int = rowData.getColumns.get("orgId").toInt
            val parentId: Int = rowData.getColumns.get("parentId").toInt
            val orgName: String = rowData.getColumns.get("orgName")
            val orgLevel: Int = rowData.getColumns.get("orgLevel").toInt

            val dimOrgDBEntity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
            val entity: String = JSON.toJSONString(dimOrgDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            println(entity)

            jedis.hset("itcast_shop:dim_org", orgId.toString, entity)
          }
          case "itcast_shop_cats" => {
            val catId: String = rowData.getColumns.get("catId")
            val parentId: String = rowData.getColumns.get("parentId")
            val catName: String = rowData.getColumns.get("catName")
            val catSort: String = rowData.getColumns.get("catSort")

            val dimShopCatDBEntity = DimShopCatDBEntity(catId, parentId, catName, catSort)
            val entity: String = JSON.toJSONString(dimShopCatDBEntity, SerializerFeature.DisableCircularReferenceDetect)
            println(entity)

            jedis.hset("itcast_shop:dim_shop_cats", catId, entity)
          }
          case _ =>
        }
      }

      /**
        * 删除维度表数据
        *
        * @param rowData
        */
      def deleteDimData(rowData: CanalRowData): Unit = {
        rowData.getTableName match {
          case "itcast_goods" => jedis.hdel("itcast_shop:dim_goods", rowData.getColumns.get("goodsId"))
          case "itcast_shops" => jedis.hdel("itcast_shop:dim_shops", rowData.getColumns.get("shopId"))
          case "itcast_goods_cats" => jedis.hdel("itcast_shop:dim_goods_cats", rowData.getColumns.get("catId"))
          case "itcast_org" => jedis.hdel("itcast_shop:dim_org", rowData.getColumns.get("orgId"))
          case "itcast_shop_cats" => jedis.hdel("itcast_shop:dim_shop_cats", rowData.getColumns.get("catId"))

        }
      }

      // 一条一条的处理数据
      override def invoke(rowData: CanalRowData, context: SinkFunction.Context[_]): Unit = {
        rowData.getEventType match {
          case eventType if (eventType == "insert" || eventType == "update") => updateDimData(rowData)
          case "delete" => deleteDimData(rowData)
          case _ =>
        }
      }

      override def close(): Unit = {
        if (jedis.isConnected) {
          jedis.close()
        }
      }
    })
  }
}
