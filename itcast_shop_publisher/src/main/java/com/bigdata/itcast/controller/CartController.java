package com.bigdata.itcast.controller;

import com.alibaba.fastjson.JSON;
import com.bigdata.itcast.pojo.CartPojo;
import com.bigdata.itcast.service.CartService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * @ description: 使用phoenix JDBC + Mybatis整合，实现对hbase数据的查询
 * @ author: spencer
 * @ date: 2020/12/24 11:07
 */
@Api(tags = "Phnenix JDBC整合Mybatis查询hbase模块")
@RestController
public class CartController {

    // 注入CartService
    @Autowired
    CartService cartService;

    /**
     * 根据goodsId查询phoenix中购物车数据
     * @param goodsId
     * @return
     */
    @ApiOperation("根据goodsId查询phoenix中购物车数据")
    @GetMapping("/cart")
    public String getCartDataByGoodsId(
            @ApiParam(name = "goodsId", value = "商品id")
            @RequestParam("goodsId") String goodsId){
        CartPojo cartPojo = cartService.getCartDataByGoodsId(goodsId);

        String result = JSON.toJSONString(cartPojo);
        System.out.println(result);

        return result;
    }

    /**
     * 查询phoenix中全部购物车数据
     * @return
     */
    @ApiOperation("查询phoenix中全部购物车数据")
    @GetMapping("/carts")
    public String getCartData(){
        List<CartPojo> cartDataList = cartService.getCartData();
        String result = JSON.toJSONString(cartDataList);
        System.out.println(result);
        return result;
    }

    /**
     * 根据userId，查询该用户相关数据的条数
     * @param userId
     * @return
     */
    @ApiOperation("根据userId，查询该用户相关数据的条数")
    @GetMapping("/cart/{userId}")
    public String getCartDataByUserId(
            @ApiParam(name = "userId", value = "用户id")
            @PathVariable("userId") String userId){
        int counts = cartService.getCartCountsByUserId(userId);
        String result = userId + ": " + counts;
        return result;
    }

    /**
     * 查询phoenix中数据条数
     * @return
     */
    @ApiOperation("查询phoenix中数据条数")
    @GetMapping("/cart/counts")
    public String getCartCounts(){
        int cartCounts = cartService.getCartCounts();
        String result = "数据总数：" + cartCounts;
        return result;
    }

}
