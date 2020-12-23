package com.itcast.shop.realtime.etl.bean

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.beans.BeanProperty

/**
  * @ description: 评论数据样例类
  * @ author: spencer
  * @ date: 2020/12/17 15:23
  */
case class CommentsEntity (
                            userId: String,
                            userName: String,
                            orderGoodsId: String,
                            startScore: Int,
                            comments: String,
                            assetsVideoJson: String,
                            goodsId: String,
                            timestamp: Long
                          )

object CommentsEntity{
  def apply(json: String): CommentsEntity = {
    val jSONObject: JSONObject = JSON.parseObject(json)
    CommentsEntity(
      jSONObject.getString("userId"),
      jSONObject.getString("userName"),
      jSONObject.getString("orderGoodsId"),
      jSONObject.getInteger("startScore"),
      jSONObject.getString("comments"),
      jSONObject.getString("assetsVideoJson"),
      jSONObject.getString("goodsId"),
      jSONObject.getLong("timestamp")
    )
  }
}

case class CommentsWideEntity(
                              @BeanProperty userId: String,
                              @BeanProperty userName: String,
                              @BeanProperty orderGoodsId: String,
                              @BeanProperty startScore: Int,
                              @BeanProperty comments: String,
                              @BeanProperty assetsVideoJson: String,
                              @BeanProperty var createTime: String,
                              @BeanProperty goodsId: String,
                              @BeanProperty var goodsName: String,
                              @BeanProperty var shopId: Long
                             )

object CommentsWideEntity{
  def apply(commentsEntity: CommentsEntity): CommentsWideEntity = {
    CommentsWideEntity(
      commentsEntity.userId,
      commentsEntity.userName,
      commentsEntity.orderGoodsId,
      commentsEntity.startScore,
      commentsEntity.comments,
      commentsEntity.assetsVideoJson,
      "",
      commentsEntity.goodsId,
      "",
      0
    )
  }
}