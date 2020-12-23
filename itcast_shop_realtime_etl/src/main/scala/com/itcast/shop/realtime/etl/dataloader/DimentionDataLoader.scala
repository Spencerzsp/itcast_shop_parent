package com.itcast.shop.realtime.etl.dataloader

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.shop.realtime.etl.bean._
import com.itcast.shop.realtime.etl.utils.{GlobalConfigUtil, RedisUtil}
import redis.clients.jedis.{Jedis, JedisPool}

/**
  * @ description: 加载mysql中的维度表到redis中
  * @ author: spencer
  * @ date: 2020/12/10 16:42
  */
object DimentionDataLoader {

  def main(args: Array[String]): Unit = {
    // 1.注册mysql驱动
    Class.forName("com.mysql.jdbc.Driver")

    // 2.创建连接
    val connection: Connection = DriverManager.getConnection(
      s"jdbc:mysql://${GlobalConfigUtil.mysql_server_ip}:${GlobalConfigUtil.mysql_server_port}/${GlobalConfigUtil.mysql_server_database}",
      GlobalConfigUtil.mysql_server_username,
      GlobalConfigUtil.mysql_server_password
    )

    // 3.创建jedis连接
    val jedis: Jedis = RedisUtil.getJedis()

    // redis中默认有16个数据库，需要指定一下维度数据到存到哪个数据库
    jedis.select(1);

    // 4.加载维度数据到redis中
    // 1) 商品维度表
    loadDimGoods(connection, jedis)

    // 2) 商品分类维度表
    loadDimGoodsCats(connection, jedis)

    // 3) 店铺表
    loadDimShops(connection, jedis)

    // 4) 组织机构表
    loadDimoOrg(connection, jedis)

    // 5) 门店商品分类表
    loadDimShopCat(connection, jedis)

    jedis.shutdown()
  }

  /**
    * 商品维度表
    * @param connection
    * @param jedis
    */
  def loadDimGoods(connection: Connection, jedis: Jedis): Unit = {
    val sql =
      """
        |select
        | goodsId,
        | goodsName,
        | shopId,
        | goodsCatId,
        | shopPrice
        |from itcast_goods
      """.stripMargin
    val pstmt: PreparedStatement = connection.prepareStatement(sql)
    val resultSet: ResultSet = pstmt.executeQuery()
    while (resultSet.next()){
      val goodsId: Long = resultSet.getLong("goodsId")
      val goodsName: String = resultSet.getString("goodsName")
      val shopId: Long = resultSet.getLong("shopId")
      val goodsCatId: Int = resultSet.getInt("goodsCatId")
      val shopPrice: Double = resultSet.getDouble("shopPrice")

      val dimGoodsDBEntity = DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId, shopPrice)
      val entity: String = JSON.toJSONString(dimGoodsDBEntity, SerializerFeature.DisableCircularReferenceDetect)
      jedis.hset("itcast_shop:dim_goods", goodsId.toString, entity)
    }
  }

  /**
    * 商品分类维度表
    * @param connection
    * @param jedis
    */
  def loadDimGoodsCats(connection: Connection, jedis: Jedis) = {
    val sql =
      """
        |select
        | catId,
        | parentId,
        | catName,
        | cat_level
        | from itcast_goods_cats
      """.stripMargin

    val pstmt: PreparedStatement = connection.prepareStatement(sql)
    val resultSet: ResultSet = pstmt.executeQuery()
    while (resultSet.next()){
      val catId: String = resultSet.getString("catId")
      val parentId: String = resultSet.getString("parentId")
      val catName: String = resultSet.getString("catName")
      val cat_level: String = resultSet.getString("cat_level")

      val dimGoodsCatDBEntity = DimGoodsCatDBEntity(catId, parentId, catName, cat_level)

      // 将对象转换为json字符串
      // SerializerFeature.DisableCircularReferenceDetect:防止循环引用抛出异常
      val entity: String = JSON.toJSONString(dimGoodsCatDBEntity, SerializerFeature.DisableCircularReferenceDetect)
      println(entity)

      // 将数据写入redis
      jedis.hset("itcast_shop:dim_goods_cats", catId, entity)
    }
    resultSet.close()
    pstmt.close()
  }

  /**
    * 店铺维度表
    * @param connection
    * @param jedis
    */
  def loadDimShops(connection: Connection, jedis: Jedis): Unit = {
    val sql =
      """
        |select
        | shopId,
        | areaId,
        | shopName,
        | shopCompany
        |from itcast_shops
      """.stripMargin

    val pstmt: PreparedStatement = connection.prepareStatement(sql)
    val resultSet: ResultSet = pstmt.executeQuery()
    while (resultSet.next()){
      val shopId: Int = resultSet.getInt("shopId")
      val areaId: Int = resultSet.getInt("areaId")
      val shopName: String = resultSet.getString("shopName")
      val shopCompany: String = resultSet.getString("shopCompany")

      val dimShopDBEntity = DimShopDBEntity(shopId, areaId, shopName, shopCompany)

      val entity: String = JSON.toJSONString(dimShopDBEntity, SerializerFeature.DisableCircularReferenceDetect)
      jedis.hset("itcast_shop:dim_shops", shopId.toString, entity)
    }
    resultSet.close()
    pstmt.close()

  }

  /**
    * 组织机构维度表
    * @param connection
    * @param jedis
    */
  def loadDimoOrg(connection: Connection, jedis: Jedis): Unit = {
    val sql =
      """
        |select
        | orgId,
        | parentId,
        | orgName,
        | orgLevel
        |from itcast_org
      """.stripMargin
    val pstmt: PreparedStatement = connection.prepareStatement(sql)
    val resultSet: ResultSet = pstmt.executeQuery()
    while (resultSet.next()){
      val orgId: Int = resultSet.getInt("orgId")
      val parentId: Int = resultSet.getInt("parentId")
      val orgName: String = resultSet.getString("orgName")
      val orgLevel: Int = resultSet.getInt("orgLevel")

      val dimOrgDBEntity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
      val entity: String = JSON.toJSONString(dimOrgDBEntity, SerializerFeature.DisableCircularReferenceDetect)

      jedis.hset("itcast_shop:dim_org", orgId.toString, entity)
    }
    resultSet.close()
    pstmt.close()
  }

  /**
    * 门店分类维度表
    * @param connection
    * @param jedis
    */
  def loadDimShopCat(connection: Connection, jedis: Jedis): Unit = {
    val sql =
      """
        |select
        | catId,
        | parentId,
        | catName,
        | catSort
        |from itcast_shop_cats
      """.stripMargin
    val pstmt: PreparedStatement = connection.prepareStatement(sql)
    val resultSet: ResultSet = pstmt.executeQuery()
    while (resultSet.next()){
      val catId: String = resultSet.getString("catId")
      val parentId: String = resultSet.getString("parentId")
      val catName: String = resultSet.getString("catName")
      val catSort: String = resultSet.getString("catSort")

      val dimShopCatDBEntity = DimShopCatDBEntity(catId, parentId, catName, catSort)
      val entity: String = JSON.toJSONString(dimShopCatDBEntity, SerializerFeature.DisableCircularReferenceDetect)

      jedis.hset("itcast_shop:dim_shop_cats", catId, entity)
    }

    resultSet.close()
    pstmt.close()
  }


}
