package com.bigdata.itcast.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.bigdata.itcast.pojo.CartPojo;
import com.bigdata.itcast.service.CartES2HBaseService;
import io.swagger.annotations.Api;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @ description: ES+HBase二级索引，实现快速搜索hbase中的数据
 * @ author: spencer
 * @ date: 2020/12/24 15:59
 */
@Api(tags = "ES+HBase二级索引查询模块")
@RestController
public class CartES2HBaseController {

    @Autowired
    CartES2HBaseService cartES2HBaseService;

    /**
     * 查询某个索引的全部数据
     *
     * @param indexName
     * @return
     */
    @GetMapping("/cart_es_hbase")
    public List<Map<String, Object>> getCartDataFromES(@RequestParam("indexName") String indexName) {

        return cartES2HBaseService.getCartDataFromES(indexName);
//        List<CartPojo> cartPojos = new ArrayList<>();
//        List<String> cartJsonList = cartES2HBaseService.getCartDataFromES(indexName);
//        CartPojo cartPojo = new CartPojo();
//        for (String cartJson : cartJsonList) {
//            JSONObject jsonObject = JSON.parseObject(cartJson);
//            String rowid = jsonObject.getString("rowid");
//            String goodsId = jsonObject.getString("goodsId");
//            String ShopName = jsonObject.getString("ShopName");
//            String goodsName = jsonObject.getString("goodsName");
//
//            cartPojo.setRowid(rowid);
//            cartPojo.setRowid(goodsId);
//            cartPojo.setRowid(ShopName);
//            cartPojo.setRowid(goodsName);
//
//            cartPojos.add(cartPojo);
//        }
//        return cartPojos;
    }

    /**
     * 根据关键词查询es中的索引数据，返回文档id的集合
     *
     * @param indexName
     * @param keyword
     * @return
     */
    @GetMapping("/cart_es/{indexName}/{keyword}")
    public List<String> getCartDataByKeyword(@PathVariable("indexName") String indexName,
                                             @PathVariable("keyword") String keyword) {
        List<String> idList = cartES2HBaseService.getCartDataByKeyword(indexName, keyword);
        return idList;

    }

    @GetMapping("/cart_es_hbase/{indexName}/{keyword}")
    public List<String> getCartDataFromHBase(@PathVariable("indexName") String indexName,
                                       @PathVariable("keyword") String keyword) {
        List<String> list = cartES2HBaseService.getCartDataFromHBase(indexName, keyword);
        System.out.println(list);

//        /**
//         * 获取jsonObject中字段对应值
//         */
//        for (String data : list) {
//            JSONObject jsonObject = JSONObject.parseObject(data);
//            String addTime = jsonObject.getString("addTime");
//            String clientProvince = jsonObject.getString("clientProvince");
//            String shopName = jsonObject.getString("shopName");
//
//            System.out.println(addTime);
//            System.out.println(clientProvince);
//            System.out.println(shopName);
//        }
        return list;
    }
}
