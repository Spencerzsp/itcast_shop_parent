package com.bigdata.itcast.service;

import com.bigdata.itcast.pojo.CartPojo;

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
}
