package com.bigdata.itcast.service.impl;

import com.bigdata.itcast.mapper.CartMapper;
import com.bigdata.itcast.pojo.CartPojo;
import com.bigdata.itcast.service.CartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/24 11:04
 */
@Service
public class CartServiceImpl implements CartService {
    // 注入CartMapper
    @Autowired
    CartMapper cartMapper;

    /**
     * 根据goodsId查询数据
     * @param goodsId
     * @return
     */
    @Override
    public CartPojo getCartDataByGoodsId(String goodsId) {
        CartPojo cartData = cartMapper.getCartDataByGoodsId(goodsId);
        return cartData;
    }
}
