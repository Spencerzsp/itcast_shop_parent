package com.itcast.shop.realtime.etl.process

import java.io.File

import com.alibaba.fastjson.serializer.SerializerFeature
import com.alibaba.fastjson.{JSON, JSONObject}
import com.itcast.canal.util.IPSeeker
import com.itcast.shop.realtime.etl.`trait`.MQBaseETL
import com.itcast.shop.realtime.etl.bean._
import com.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, HBaseUtil, RedisUtil}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import redis.clients.jedis.Jedis

/**
  * @ description: 购物车数据的实时ETL
  * {"addTime":1576479746005,"count":1,"goodsId":"100106","guid":"f1eeb1d9-9eec-88da-61f87ab0302c","ip":"123.125.71.102","userId":"100208"}
  * @ author: spencer
  * @ date: 2020/12/17 14:13
  */
class CartDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env){
  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    */
  override def process(): Unit = {
    val cartJsonDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.input_topic_cart)
    val cartEntityDataStream: DataStream[CartEntity] = cartJsonDataStream.map(cartJsonData => {
      CartEntity(cartJsonData)
    })

    val cartWideDataStream: DataStream[CartWideEntity] = cartEntityDataStream.map(new RichMapFunction[CartEntity, CartWideEntity] {

      var jedis: Jedis = _
      var seeker: IPSeeker = _

      override def open(parameters: Configuration): Unit = {
        val file: File = getRuntimeContext.getDistributedCache.getFile("qqwry.dat")
        seeker = new IPSeeker(file)
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      override def close(): Unit = {
        if (jedis.isConnected) {
          jedis.close()
        }
      }

      override def map(cartEntity: CartEntity): CartWideEntity = {
        val cartWideEntity = CartWideEntity(cartEntity)

        // 根据拉宽后的商品id获取redis中商品详细数据
        val goodsJson: String = jedis.hget("itcast_shop:dim_goods", cartWideEntity.goodsId)
        val dimGoods = DimGoodsDBEntity(goodsJson)

        // 获取商品三级分类数据
        val goodsCat3Json: String = jedis.hget("itcast_shop:dim_goods_cats", dimGoods.goodsCatId.toString)
        val dimGoodsCat3 = DimGoodsCatDBEntity(goodsCat3Json)

        // 获取商品二级分类数据
        val goodsCat2Json: String = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsCat3.parentId)
        val dimGoodsCat2 = DimGoodsCatDBEntity(goodsCat2Json)

        // 获取商品一级分类数据
        val goodsCat1Json: String = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsCat2.parentId)
        val dimGoodsCat1 = DimGoodsCatDBEntity(goodsCat1Json)

        // 获取商品店铺数据
        val shopJson: String = jedis.hget("itcast_shop:dim_shops", dimGoods.shopId.toString)
        val dimShop = DimShopDBEntity(shopJson)

        // 获取店铺管理所属城市数据
        val cityJson: String = jedis.hget("itcast_shop:dim_org", dimShop.areaId.toString)
        val dimCity = DimOrgDBEntity(cityJson)

        // 获取店铺管理所属省份数据
        val provinceJson: String = jedis.hget("itcast_shop:dim_org", dimCity.parentId.toString)
        val dimProvince = DimOrgDBEntity(provinceJson)

        // 封装拉宽后的购物车数据
        cartWideEntity.goodsPrice = dimGoods.shopPrice
        cartWideEntity.goodsName = dimGoods.goodsName
        cartWideEntity.goodsCat3 = dimGoodsCat3.catName
        cartWideEntity.goodsCat2 = dimGoodsCat2.catName
        cartWideEntity.goodsCat1 = dimGoodsCat1.catName
        cartWideEntity.shopId = dimShop.shopId.toString
        cartWideEntity.shopName = dimShop.shopName
        cartWideEntity.shopProvinceId = dimProvince.orgId.toString
        cartWideEntity.shopProvinceName = dimProvince.orgName
        cartWideEntity.shopCityId = dimCity.orgId.toString
        cartWideEntity.shopCityName = dimCity.orgName

        // 解析ip数据
        val country: String = seeker.getCountry(cartWideEntity.ip)
        var areaArray: Array[String] = country.split("省")
        if (areaArray.length > 1) {
          cartWideEntity.clientProvince = areaArray(0) + "省"
          cartWideEntity.clientCity = areaArray(1)
        } else { // 直辖市
          areaArray = country.split("市")
          if (areaArray.length > 1) {
            cartWideEntity.clientProvince = areaArray(0) + "市"
            cartWideEntity.clientCity = areaArray(1)
          } else {
            cartWideEntity.clientProvince = areaArray(0)
            cartWideEntity.clientCity = ""
          }
        }
        cartWideEntity
      }
    })
    cartWideDataStream.print("拉宽后的购物车数据：")

    // 将拉宽后的数据转换为json字符串写入kafka
    val cartWideJson: DataStream[String] = cartWideDataStream.map(cartWide => {
      JSON.toJSONString(cartWide, SerializerFeature.DisableCircularReferenceDetect)
    })
    cartWideJson.addSink(kafkaProducer(GlobalConfigUtil.output_topic_cart))

    // 將拉宽后的数据写入hbase
    cartWideDataStream.addSink(new RichSinkFunction[CartWideEntity] {

      var connection: Connection = _
      var table: Table = _
      override def open(parameters: Configuration): Unit = {
        connection = HBaseUtil.getConnection()
        table = connection.getTable(TableName.valueOf("dwd_itcast_cart"))
      }

      override def invoke(cartWideEntity: CartWideEntity, context: SinkFunction.Context[_]): Unit = {
        // 构建put对象
        val rowKey = cartWideEntity.addTime + "_" + cartWideEntity.userId
        val put = new Put(Bytes.toBytes(rowKey))

        // 构建列名
        val goodsId: Array[Byte] = Bytes.toBytes("goodsId")
        val userId: Array[Byte] = Bytes.toBytes("userId")
        val count: Array[Byte] = Bytes.toBytes("count")
        val guid: Array[Byte] = Bytes.toBytes("guid")
        val addTime: Array[Byte] = Bytes.toBytes("addTime")
        val ip: Array[Byte] = Bytes.toBytes("ip")
        val goodsPrice: Array[Byte] = Bytes.toBytes("goodsPrice")
        val goodsName: Array[Byte] = Bytes.toBytes("goodsName")
        val goodsCat3: Array[Byte] = Bytes.toBytes("goodsCat3")
        val goodsCat2: Array[Byte] = Bytes.toBytes("goodsCat2")
        val goodsCat1: Array[Byte] = Bytes.toBytes("goodsCat1")
        val shopId: Array[Byte] = Bytes.toBytes("shopId")
        val shopName: Array[Byte] = Bytes.toBytes("shopName")
        val shopProvinceId: Array[Byte] = Bytes.toBytes("shopProvinceId")
        val shopProvinceName: Array[Byte] = Bytes.toBytes("shopProvinceName")
        val shopCityId: Array[Byte] = Bytes.toBytes("shopCityId")
        val shopCityName: Array[Byte] = Bytes.toBytes("shopCityName")
        val clientProvince: Array[Byte] = Bytes.toBytes("clientProvince")
        val clientCity: Array[Byte] = Bytes.toBytes("clientCity")

        // 构建列族名称
        val cart: Array[Byte] = Bytes.toBytes("cart")

        // 封装列
        put.addColumn(cart, goodsId, Bytes.toBytes(cartWideEntity.goodsId))
        put.addColumn(cart, userId, Bytes.toBytes(cartWideEntity.userId))
        put.addColumn(cart, count, Bytes.toBytes(cartWideEntity.count.toString))
        put.addColumn(cart, guid, Bytes.toBytes(cartWideEntity.guid))
        put.addColumn(cart, addTime, Bytes.toBytes(cartWideEntity.addTime))
        put.addColumn(cart, ip, Bytes.toBytes(cartWideEntity.ip))
        put.addColumn(cart, goodsPrice, Bytes.toBytes(cartWideEntity.goodsPrice.toString))
        put.addColumn(cart, goodsName, Bytes.toBytes(cartWideEntity.goodsName))
        put.addColumn(cart, goodsCat3, Bytes.toBytes(cartWideEntity.goodsCat3))
        put.addColumn(cart, goodsCat2, Bytes.toBytes(cartWideEntity.goodsCat2))
        put.addColumn(cart, goodsCat1, Bytes.toBytes(cartWideEntity.goodsCat1))
        put.addColumn(cart, shopId, Bytes.toBytes(cartWideEntity.shopId))
        put.addColumn(cart, shopName, Bytes.toBytes(cartWideEntity.shopName))
        put.addColumn(cart, shopProvinceId, Bytes.toBytes(cartWideEntity.shopProvinceId))
        put.addColumn(cart, shopProvinceName, Bytes.toBytes(cartWideEntity.shopProvinceName))
        put.addColumn(cart, shopCityId, Bytes.toBytes(cartWideEntity.shopCityId))
        put.addColumn(cart, shopCityName, Bytes.toBytes(cartWideEntity.shopCityName))
        put.addColumn(cart, clientProvince, Bytes.toBytes(cartWideEntity.clientProvince))
        put.addColumn(cart, clientCity, Bytes.toBytes(cartWideEntity.clientCity))

        // 执行put操作，写入hbase
        table.put(put)
      }

      override def close(): Unit = {
        if (table != null) table.close()
        if (connection.isClosed) connection.close()
      }
    })
  }
}
