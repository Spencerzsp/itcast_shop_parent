package com.bigdata.itcast.controller;

import com.alibaba.fastjson.JSON;
import com.bigdata.itcast.pojo.CartPojo;
import com.bigdata.itcast.service.CartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/24 11:07
 */
@RestController
public class CartController {

    // 注入CartService
    @Autowired
    CartService cartService;

    @GetMapping("/cart")
    public String getCartDataByGoodsId(@RequestParam("goodsId") String goodsId){
        CartPojo cartPojo = cartService.getCartDataByGoodsId(goodsId);

        String result = JSON.toJSONString(cartPojo);
        System.out.println(result);

        return result;
    }
}
