package com.itcast.shop.realtime.etl.bean

import scala.beans.BeanProperty

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/15 16:31
  */
case class OrderGoodsWideDBEntity(
                                 @BeanProperty ogId: Long,
                                 @BeanProperty orderId: Long,
                                 @BeanProperty goodsId: Long,
                                 @BeanProperty goodsNum: Long,
                                 @BeanProperty goodsPrice: Double,
                                 @BeanProperty goodsName: String,
                                 @BeanProperty shopId: Long,
                                 @BeanProperty goodsThirdCatId: Int,
                                 @BeanProperty goodsThirdCatName: String,
                                 @BeanProperty goodsSecondCatId: Int,
                                 @BeanProperty goodsSecondCatName: String,
                                 @BeanProperty goodsFirstCatId: Int,
                                 @BeanProperty goodsFirstCatName: String,
                                 @BeanProperty areaId: Int,
                                 @BeanProperty shopName: String,
                                 @BeanProperty shopCompany: String,
                                 @BeanProperty cityId: Int,
                                 @BeanProperty cityName: String,
                                 @BeanProperty regionId: Int,
                                 @BeanProperty regionName: String
                                 )