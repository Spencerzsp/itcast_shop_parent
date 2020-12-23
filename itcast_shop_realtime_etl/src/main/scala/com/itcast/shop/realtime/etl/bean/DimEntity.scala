package com.itcast.shop.realtime.etl.bean

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.beans.BeanProperty

/**
  * @ description: 定义维度表的样例类
  * @ author: spencer
  * @ date: 2020/12/10 15:12
  */
class DimEntity {
}

/**
  * 商品维度样例类
  *
  * @param goodsId    商品id
  * @param goodsName  商品名称
  * @param shopId     店铺id
  * @param goodsCatId 商品分类id
  * @param shopPrice  商品价格
  */
case class DimGoodsDBEntity(@BeanProperty goodsId: Long = 0,
                            @BeanProperty goodsName: String = "",
                            @BeanProperty shopId: Long = 0,
                            @BeanProperty goodsCatId: Int = 0,
                            @BeanProperty shopPrice: Double = 0)

/**
  * 商品的伴生对象
  */
object DimGoodsDBEntity {
  def apply(json: String): DimGoodsDBEntity = {
    if (json != null) {
      val jSONObject: JSONObject = JSON.parseObject(json)
      DimGoodsDBEntity(
        jSONObject.getLong("goodsId"),
        jSONObject.getString("goodsName"),
        jSONObject.getLong("shopId"),
        jSONObject.getInteger("goodsCatId"),
        jSONObject.getDouble("shopPrice")
      )
    } else {
      new DimGoodsDBEntity()
    }
  }
}

/**
  * 商品分类维度样例类
  *
  * @param catId     商品分类id
  * @param parentId  商品分类父id
  * @param catName   商品分类名称
  * @param cat_level 商品分类级别
  */
case class DimGoodsCatDBEntity(@BeanProperty catId: String = "",
                               @BeanProperty parentId: String = "",
                               @BeanProperty catName: String = "",
                               @BeanProperty cat_level: String = "")

/**
  * 商品分类维度表的伴生对象
  */
object DimGoodsCatDBEntity {
  def apply(json: String): DimGoodsCatDBEntity = {
    if (json != null) {
      val jSONObject: JSONObject = JSON.parseObject(json)
      val catId: String = jSONObject.getString("catId")
      val parentId: String = jSONObject.getString("parentId")
      val catName: String = jSONObject.getString("catName")
      val cat_level: String = jSONObject.getString("cat_level")

      DimGoodsCatDBEntity(
        catId,
        parentId,
        catName,
        cat_level
      )
    } else {
      new DimGoodsCatDBEntity()
    }
  }
}

/**
  * 店铺维度样例类
  *
  * @param shopId      店铺id
  * @param areaId      区域id
  * @param shopName    店铺名称
  * @param shopCompany 店铺公司
  */
case class DimShopDBEntity(@BeanProperty shopId: Int = 0,
                           @BeanProperty areaId: Int = 0,
                           @BeanProperty shopName: String = "",
                           @BeanProperty shopCompany: String = "")

object DimShopDBEntity {
  def apply(json: String): DimShopDBEntity = {
    if (json != null) {
      val jSONObject: JSONObject = JSON.parseObject(json)
      val shopId: Integer = jSONObject.getInteger("shopId")
      val areaId: Integer = jSONObject.getInteger("areaId")
      val shopName: String = jSONObject.getString("shopName")
      val shopCompany: String = jSONObject.getString("shopCompany")

      DimShopDBEntity(
        shopId,
        areaId,
        shopName,
        shopCompany
      )
    } else {
      new DimShopDBEntity()
    }
  }
}

/**
  * 组织机构维度样例类
  *
  * @param orgId    机构id
  * @param parentId 机构父id
  * @param orgName  机构名称
  * @param orgLevel 机构级别
  */
case class DimOrgDBEntity(@BeanProperty orgId: Int = 0,
                          @BeanProperty parentId: Int = 0,
                          @BeanProperty orgName: String = "",
                          @BeanProperty orgLevel: Int = 0)

object DimOrgDBEntity {
  def apply(json: String): DimOrgDBEntity = {
    if (json != null) {
      val jSONObject: JSONObject = JSON.parseObject(json)
      val orgId: Integer = jSONObject.getInteger("orgId")
      val parentId: Integer = jSONObject.getInteger("parentId")
      val orgName: String = jSONObject.getString("orgName")
      val orgLevel: Integer = jSONObject.getInteger("orgLevel")

      DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
    } else {
      new DimOrgDBEntity()
    }
  }
}

/**
  * 店铺分类维度样例类
  * @param catId 分类id
  * @param parentId 分类父id
  * @param catName 分类名称
  * @param catSort 分类级别
  */
case class DimShopCatDBEntity(@BeanProperty catId: String = "",
                              @BeanProperty parentId: String = "",
                              @BeanProperty catName: String = "",
                              @BeanProperty catSort: String = "")

object DimShopCatDBEntity{
  def apply(json: String): DimShopCatDBEntity = {
    if (json != null){
      val jSONObject: JSONObject = JSON.parseObject(json)
      val catId: String = jSONObject.getString("catId")
      val parentId: String = jSONObject.getString("parentId")
      val catName: String = jSONObject.getString("catName")
      val catSort: String = jSONObject.getString("catSort")

      DimShopCatDBEntity(catId, parentId, catName,catSort)
    } else {
      new DimShopCatDBEntity()
    }
  }
}







