package com.itcast.shop.realtime.etl.async

import java.util

import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.bean._
import com.itcast.shop.realtime.etl.utils.RedisUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}
import redis.clients.jedis.Jedis

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor, Future}

/**
  * @ description: 异步查询订单明细数据和维度数据进行关联(维度数据在redis中)
  * @ author: spencer
  * @ date: 2020/12/15 17:00
  */
class AsyncOrderDetailRedisRequest() extends RichAsyncFunction[CanalRowData, OrderGoodsWideDBEntity]{

  var jedis: Jedis = _
  override def open(parameters: Configuration): Unit = {
    jedis = RedisUtil.getJedis()
    jedis.select(1)
  }


  /**
    * 连接redis超时的操作，默认会抛出异常，一旦重写了该方法，则会执行方法中的逻辑
    * @param input
    * @param resultFuture
    */
  override def timeout(input: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideDBEntity]): Unit = {
    println("订单明细数据关联redis中的维度数据超时了")
  }


  // 定义异步回调的上下文对象
  implicit lazy val executor: ExecutionContextExecutor = ExecutionContext.fromExecutor(Executors.directExecutor())

  /**
    * 异步操作，对数据流中的每一条数据进行处理
    * @param rowData
    * @param resultFuture
    */
  override def asyncInvoke(rowData: CanalRowData, resultFuture: ResultFuture[OrderGoodsWideDBEntity]): Unit = {

    // 发起异步请求，获取请求结束的Future
    Future {
      // 1.根据商品id获取商品的详细信息
      val goodsJson: String = jedis.hget("itcast_shop:dim_goods", rowData.getColumns.get("goodsId"))
      //将商品详细信息的json数据转换成商品的样例类
      val dimGoodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsJson)

      // 2.根据商品表的店铺id，获取店铺的详细信息
      val shopJson: String = jedis.hget("itcast_shop:dim_shops", dimGoodsDBEntity.shopId.toString)
      val dimShopDBEntity = DimShopDBEntity(shopJson)

      // 3.根据商品表的分类id，获取商品分类的详细信息(三级分类)
      val thirdCatJson: String = jedis.hget("itcast_shop:dim_goods_cats", dimGoodsDBEntity.goodsCatId.toString)
      val dimThirdCat: DimGoodsCatDBEntity = DimGoodsCatDBEntity(thirdCatJson)

      // 4.根据三级分类id，获取商品分类的详细信息(二级分类)
      val secondCatJson: String = jedis.hget("itcast_shop:dim_goods_cats", dimThirdCat.parentId)
      val dimSecondCat = DimGoodsCatDBEntity(secondCatJson)

      // 5.根据二级分类id，获取商品分类的详细信息(一级分类)
      val firstCatJson: String = jedis.hget("itcast_shop:dim_goods_cats", dimSecondCat.parentId)
      val dimFirstCat = DimGoodsCatDBEntity(firstCatJson)

      // 6.根据店铺表的区域id，找到组织机构数据
      // 6.1根据店铺的areaId，找到组织机构的详细数据
      val orgJson: String = jedis.hget("itcast_shop:dim_org", dimShopDBEntity.areaId.toString)
      val dimOrgDBEntity = DimOrgDBEntity(orgJson)

      // 6.2根据区域的父id获取大区数据
      val regionJson: String = jedis.hget("itcast_shop:dim_org", dimOrgDBEntity.parentId.toString)
      val dimRegion = DimOrgDBEntity(regionJson)

      // 构建明细订单宽表数据
      val orderGoodsWideDBEntity = OrderGoodsWideDBEntity(
        rowData.getColumns.get("ogId").toLong,
        rowData.getColumns.get("orderId").toLong,
        rowData.getColumns.get("goodsId").toLong,
        rowData.getColumns.get("goodsNum").toLong,
        rowData.getColumns.get("goodsPrice").toDouble,
        rowData.getColumns.get("goodsName"),
        dimShopDBEntity.shopId.toLong,
        dimThirdCat.catId.toInt,
        dimThirdCat.catName,
        dimSecondCat.catId.toInt,
        dimSecondCat.catName,
        dimFirstCat.catId.toInt,
        dimFirstCat.catName,
        dimShopDBEntity.areaId,
        dimShopDBEntity.shopName,
        dimShopDBEntity.shopCompany,
        dimOrgDBEntity.orgId,
        dimOrgDBEntity.orgName,
        dimRegion.orgId,
        dimRegion.orgName
      )


      // 异步请求回调
      resultFuture.complete(Array(orderGoodsWideDBEntity))
    }
  }

  override def close(): Unit = {
    if (jedis.isConnected){
      jedis.close()
    }
  }
}
