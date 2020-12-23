package com.itcast.shop.realtime.etl.async

import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.bean._
import com.itcast.shop.realtime.etl.utils.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/16 16:16
  */
class AsyncGoodsRedisRequest() extends RichAsyncFunction[CanalRowData, GoodsWideEntity]{
  var jedis: Jedis = _

  override def open(parameters: Configuration): Unit = {
    jedis = RedisUtil.getJedis()
    jedis.select(1)
  }

  // 定义回调的上下文
  implicit lazy val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.directExecutor())

  override def timeout(input: CanalRowData, resultFuture: ResultFuture[GoodsWideEntity]): Unit = {
    println("连接redis超时了")
  }

  override def asyncInvoke(rowData: CanalRowData, resultFuture: ResultFuture[GoodsWideEntity]): Unit = {
    Future{
      // 根据店铺id获取店铺的详细信息
      val shopJson: String = jedis.hget("itcast_shop:dim_shops", rowData.getColumns.get("shopId"))
      val dimShop = DimShopDBEntity(shopJson)

      // 获取三级分类的相关信息
      val goodsThirdCatsJson: String = jedis.hget("itcast_shop:dim_goods_cats", rowData.getColumns.get("goodsCatId"))
      val dimGoodsThirdCat = DimGoodsCatDBEntity(goodsThirdCatsJson)

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
      val shopCat2Json: String = jedis.hget("itcast_shop:dim_shop_cats", rowData.getColumns.get("shopCatId2"))
      val shopCat2 = DimShopCatDBEntity(shopCat2Json)
      val shopCatId2: String = shopCat2.catId

      val cityJson: String = jedis.hget("itcast_shop:dim_org", dimShop.areaId.toString)
      val dimCity = DimOrgDBEntity(cityJson)

      val regionJson: String = jedis.hget("itcast_shop:dim_org", dimCity.parentId.toString)
      val dimRegion = DimOrgDBEntity(regionJson)

      val goodsWide = GoodsWideEntity(
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
        rowData.getColumns.get("goodsSerachKeywords"),
        rowData.getColumns.get("modifyTime"),
        dimCity.orgId,
        dimCity.orgName,
        dimRegion.orgId,
        dimRegion.orgName
      )

      resultFuture.complete(Array(goodsWide))
    }
  }

  override def close(): Unit = {
    if (jedis.isConnected){
      jedis.close()
    }
  }
}
