package com.bigdata.itcast.service;

import com.alibaba.fastjson.JSONArray;
import com.bigdata.itcast.pojo.CartPojo;

import java.util.List;
import java.util.Map;

/**
 * @ description: ES+HBASE实现二级索引查询购物车数据
 * @ author: spencer
 * @ date: 2020/12/24 15:02
 */
public interface CartES2HBaseService {

    /**
     * 查询es中全部数据
     * @return
     */
    List<Map<String, Object>> getCartDataFromES(String indexName);

    /**
     * 根据关键词查询es中的数据，并返回文档的id(即hbase中对应的rowkey)
     * @param indexName
     * @param keyword
     * @return
     */
    List<String> getCartDataByKeyword(String indexName, String keyword);

    /**
     * 根据rowkey查询hbase中的数据
     * @param indexName
     * @param keyword
     * @return
     */
    List<String> getCartDataFromHBase(String indexName, String keyword);
}
