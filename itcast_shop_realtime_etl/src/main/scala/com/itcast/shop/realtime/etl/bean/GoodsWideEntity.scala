package com.itcast.shop.realtime.etl.bean

import scala.beans.BeanProperty

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/16 14:53
  */
case class GoodsWideEntity (
                             @BeanProperty goodsId: Long,
                             @BeanProperty goodsSn: String,
                             @BeanProperty productNo: String,
                             @BeanProperty goodsName: String,
                             @BeanProperty goodsImg: String,
                             @BeanProperty shopId: String,
                             @BeanProperty shopName: String,
                             @BeanProperty goodsType: String,
                             @BeanProperty marketPrice: String,
                             @BeanProperty shopPrice: String,
                             @BeanProperty warnStock: String,
                             @BeanProperty goodsStock: String,
                             @BeanProperty goodsTips: String,
                             @BeanProperty isSale: String,
                             @BeanProperty isBest: String,
                             @BeanProperty isHot: String,
                             @BeanProperty isNew: String,
                             @BeanProperty isRecom: String,
                             @BeanProperty goodsCatIdPath: String,
//                             @BeanProperty goodsCatId: String,
                             @BeanProperty goodsThirdCatId: Int,
                             @BeanProperty goodsThirdCatName: String,
                             @BeanProperty goodsSecondCatId: Int,
                             @BeanProperty goodsSecondCatName: String,
                             @BeanProperty goodsFirstCatId: Int,
                             @BeanProperty goodsFirstCatName: String,
                             @BeanProperty shopCatId1: Int,
                             @BeanProperty shopCatName1: String,
                             @BeanProperty shopCatId2: Int,
                             @BeanProperty shopCatName2: String,
                             @BeanProperty brandId: String,
                             @BeanProperty goodsDesc: String,
                             @BeanProperty goodsStatus: String,
                             @BeanProperty saleNum: String,
                             @BeanProperty saleTime: String,
                             @BeanProperty visitNum: String,
                             @BeanProperty appraiseNum: String,
                             @BeanProperty isSpec: String,
                             @BeanProperty gallery: String,
                             @BeanProperty goodsSeoKeywords: String,
                             @BeanProperty illegalRemarks: String,
                             @BeanProperty dataFlag: String,
                             @BeanProperty createTime: String,
                             @BeanProperty isFreeShipping: String,
                             @BeanProperty goodsSearchKeywords: String,
                             @BeanProperty modifyTime: String,
                             @BeanProperty cityId: Int,
                             @BeanProperty cityName: String,
                             @BeanProperty regionId: Int,
                             @BeanProperty regionName: String
                           )
