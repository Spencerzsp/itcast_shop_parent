package com.itcast.shop.realtime.etl.process

import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import com.itcast.shop.realtime.etl.bean._
import com.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import redis.clients.jedis.Jedis

/**
  * @ description:
  * @ author: spencer
  * @ date: 2020/12/16 15:08
  */
class GoodsDataETL2(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    */
  override def process(): Unit = {
    /**
      * 实现步骤：
      * 1.获取kafka数据源，将CanalRowData转换成拉宽后的商品阳历类OrderWideDBEntity
      * 2.将拉宽后的OrderWideDBEntity转换成json字符串写入kafka
      * 3.将拉宽后的OrderWideDBEntity写入hbase
      */
    // 1.获取kafka数据源，将CanalRowData转换成拉宽后的商品阳历类OrderWideDBEntity
    val goodsCanalDataStream: DataStream[CanalRowData] = getKafkaDataStream().filter(_.getTableName == "itcast_goods")
    val goodsWideDataStream: DataStream[GoodsWideEntity] = goodsCanalDataStream.map(new RichMapFunction[CanalRowData, GoodsWideEntity] {
      // 定义redis连接对象
      var jedis: Jedis = _

      override def open(parameters: Configuration): Unit = {
        jedis = RedisUtil.getJedis()
        jedis.select(1)
      }

      // 关闭连接，释放资源
      override def close(): Unit = {
        if (jedis.isConnected) {
          jedis.close()
        }
      }

      // 处理一条条数据，进行拉宽操作
      override def map(rowData: CanalRowData): GoodsWideEntity = {

        // 根据店铺id获取店铺的详细信息
        val shopJson: String = jedis.hget("itcast_shop:dim_shops", rowData.getColumns.get("shopId"))
        val dimShop = DimShopDBEntity(shopJson)

        val goodsThirdCatsJson: String = jedis.hget("itcast_shop:dim_goods_cats", rowData.getColumns.get("goodsCatId"))
        val dimGoodsThirdCat = DimGoodsCatDBEntity(goodsThirdCatsJson)

        // 获取商品三级分类id
//        val dimGoodsThirdCat: DimGoodsCatDBEntity = DimGoodsCatDBEntity(goodsCatsJson)
//        val goodsThirdCatId: String = dimGoodsThirdCat.catId

        // 根据三级分类id获取二级分类id
        val goodsSecondCatsJson: String = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsThirdCat.parentId)
        val dimGoodsSecondCat = DimGoodsCatDBEntity(goodsSecondCatsJson)
        val goodsSecondCatId: String = dimGoodsSecondCat.catId

        val goodsFirstCatsJson: String = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsSecondCat.parentId)
        val dimGoodsFirstCat = DimGoodsCatDBEntity(goodsFirstCatsJson)
        val goodsFirstCatId: String = dimGoodsFirstCat.catId

        // 根据门店二级分类id，获取门店二级分类的名称
        val shopCat1Json: String = jedis.hget("itcast_shop:dim_shop_cats", rowData.getColumns.get("shopCatId1"))
        val shopCat1 = DimShopCatDBEntity(shopCat1Json)
        val shopCatId1: String = shopCat1.catId

        // 根据门店二级分类的id，获取门店一级分类的名称
        val shopCat2Json: String = jedis.hget("itcast_shop:dim_shop_cats", shopCat1.parentId)
        val shopCat2 = DimShopCatDBEntity(shopCat2Json)
        val shopCatId2: String = shopCat2.catId

        val cityJson: String = jedis.hget("itcast_shop:dim_org", dimShop.areaId.toString)
        val dimCity = DimOrgDBEntity(cityJson)

        val regionJson: String = jedis.hget("itcast_shop:dim_org", dimCity.parentId.toString)
        val dimRegion = DimOrgDBEntity(regionJson)


        GoodsWideEntity(
          rowData.getColumns.get("goodsId").toLong,
          rowData.getColumns.get("goodsSn"),
          rowData.getColumns.get("productNo"),
          rowData.getColumns.get("goodsName"),
          rowData.getColumns.get("goodsImg"),
          rowData.getColumns.get("shopId"),
          dimShop.shopName,
          rowData.getColumns.get("goodsType"),
          rowData.getColumns.get("marketPrice"),
          rowData.getColumns.get("shopPrice"),
          rowData.getColumns.get("warnStock"),
          rowData.getColumns.get("goodsStock"),
          rowData.getColumns.get("goodsTips"),
          rowData.getColumns.get("isSale"),
          rowData.getColumns.get("isBest"),
          rowData.getColumns.get("isHot"),
          rowData.getColumns.get("isNew"),
          rowData.getColumns.get("isRecom"),
          rowData.getColumns.get("goodsCatIdPath"),
          dimGoodsThirdCat.catId.toInt,
          dimGoodsThirdCat.catName,
          goodsSecondCatId.toInt,
          dimGoodsSecondCat.catName,
          goodsFirstCatId.toInt,
          dimGoodsFirstCat.catName,
          shopCatId1.toInt,
          shopCat1.catName,
          shopCatId2.toInt,
          shopCat2.catName,
          rowData.getColumns.get("brandId"),
          rowData.getColumns.get("goodsDesc"),
          rowData.getColumns.get("goodsStatus"),
          rowData.getColumns.get("saleNum"),
          rowData.getColumns.get("saleTime"),
          rowData.getColumns.get("visitNum"),
          rowData.getColumns.get("appraiseNum"),
          rowData.getColumns.get("isSpec"),
          rowData.getColumns.get("gallery"),
          rowData.getColumns.get("goodsSeoKeywords"),
          rowData.getColumns.get("illegalRemarks"),
          rowData.getColumns.get("dataFlag"),
          rowData.getColumns.get("createTime"),
          rowData.getColumns.get("isFreeShipping"),
          rowData.getColumns.get("goodsSearchKeywords"),
          rowData.getColumns.get("modifyTime"),
          dimCity.orgId,
          dimCity.orgName,
          dimRegion.orgId,
          dimRegion.orgName
        )
      }
    })

    goodsWideDataStream.print("商品详细数据：")
  }
}
