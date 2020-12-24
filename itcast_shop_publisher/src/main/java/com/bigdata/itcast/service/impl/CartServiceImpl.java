package com.bigdata.itcast.service.impl;

import com.bigdata.itcast.mapper.CartMapper;
import com.bigdata.itcast.pojo.CartPojo;
import com.bigdata.itcast.service.CartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

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

    /**
     * 查询phoenix全部数据
     * @return
     */
    @Override
    public List<CartPojo> getCartData() {
        List<CartPojo> cartDataList = cartMapper.getCartData();
        return cartDataList;
    }

    /**
     * 查询具体的userId相同的次数
     * @param userId
     * @return
     */
    @Override
    public int getCartCountsByUserId(String userId) {
        return cartMapper.getCartCountsByUserId(userId);
    }

    /**
     * 查询数据条数
     * @return
     */
    @Override
    public int getCartCounts() {
        return cartMapper.getCartCounts();
    }
}
