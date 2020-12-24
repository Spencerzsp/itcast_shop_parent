package com.bigdata.itcast.controller;

import com.alibaba.fastjson.JSON;
import com.bigdata.itcast.pojo.CartPojo;
import com.bigdata.itcast.service.CartService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

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

    @RequestMapping("/carts")
    public String getCartData(){
        List<CartPojo> cartDataList = cartService.getCartData();
        String result = JSON.toJSONString(cartDataList);
        System.out.println(result);
        return result;
    }

    @RequestMapping("/cart/{userId}")
    public String getCartDataByUserId(@PathVariable("userId") String userId){
        int counts = cartService.getCartCountsByUserId(userId);
        String result = userId + ": " + counts;
        return result;
    }

    @RequestMapping("/cart/counts")
    public String getCartCounts(){
        int cartCounts = cartService.getCartCounts();
        String result = "数据总数：" + cartCounts;
        return result;
    }
}
