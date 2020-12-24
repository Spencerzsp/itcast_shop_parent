package com.bigdata.itcast.service;

import com.bigdata.itcast.pojo.CartPojo;

import java.util.List;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/24 11:03
 */
public interface CartService {

    /**
     * 根据goodsId查询phoenix中经flink拉宽后的购物车数据
     * @param goodsId
     * @return
     */
    CartPojo getCartDataByGoodsId(String goodsId);

    /**
     * 查询phoenix全部数据
     * @return
     */
    List<CartPojo> getCartData();

    /**
     * 查询具体的userId相同的次数
     * @param userId
     * @return
     */
    int getCartCountsByUserId(String userId);

    /**
     * 查询数据条数
     * @return
     */
    int getCartCounts();
}
