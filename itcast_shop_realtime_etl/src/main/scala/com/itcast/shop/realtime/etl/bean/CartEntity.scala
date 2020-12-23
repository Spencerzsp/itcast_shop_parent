package com.itcast.shop.realtime.etl.bean

import com.alibaba.fastjson.{JSON, JSONObject}
import com.itcast.shop.realtime.etl.utils.DateUtil

import scala.beans.BeanProperty

/**
  * @ description: 创建购物车的样例类
  * @ author: spencer
  * @ date: 2020/12/17 13:42
  */
case class CartEntity(
                     goodsId: String,
                     userId: String,
                     count: Integer,
                     guid: String,
                     addTime: String,
                     ip: String
                     )

object CartEntity{
  def apply(json: String): CartEntity = {
    val jSONObject: JSONObject = JSON.parseObject(json)
    CartEntity(
      jSONObject.getString("goodsId"),
      jSONObject.getString("userId"),
      jSONObject.getInteger("count"),
      jSONObject.getString("guid"),
      jSONObject.getString("addTime"),
      jSONObject.getString("ip")
    )
  }
}

/**
  * 创建购物车拉宽后的样例类
  */
case class CartWideEntity(
                           @BeanProperty goodsId: String,
                           @BeanProperty userId: String,
                           @BeanProperty count: Integer,
                           @BeanProperty guid: String,
                           @BeanProperty addTime: String,
                           @BeanProperty ip: String,
                           @BeanProperty var goodsPrice: Double,
                           @BeanProperty var goodsName: String,
                           @BeanProperty var goodsCat3: String,
                           @BeanProperty var goodsCat2: String,
                           @BeanProperty var goodsCat1: String,
                           @BeanProperty var shopId: String,
                           @BeanProperty var shopName: String,
                           @BeanProperty var shopProvinceId: String,
                           @BeanProperty var shopProvinceName: String,
                           @BeanProperty var shopCityId: String,
                           @BeanProperty var shopCityName: String,
                           @BeanProperty var clientProvince: String,
                           @BeanProperty var clientCity: String
                         )

object CartWideEntity{
  def apply(cartEntity: CartEntity): CartWideEntity = {
    CartWideEntity(
      cartEntity.goodsId,
      cartEntity.userId,
      cartEntity.count,
      cartEntity.guid,
      DateUtil.timestampMillSeconds2str(cartEntity.addTime.toLong, "yyyy-MM-dd HH:mm:ss"),
      cartEntity.ip,
      0,
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      "",
      ""
    )
  }
}