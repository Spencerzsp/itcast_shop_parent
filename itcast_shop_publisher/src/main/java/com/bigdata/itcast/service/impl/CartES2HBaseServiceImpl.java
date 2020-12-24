package com.bigdata.itcast.service.impl;

import com.bigdata.itcast.service.CartES2HBaseService;
import com.bigdata.itcast.util.ESClientUtil;
import com.bigdata.itcast.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/24 15:10
 */
@Service
public class CartES2HBaseServiceImpl implements CartES2HBaseService {

    @Autowired
    ESClientUtil esClientUtil;

    @Autowired
    HBaseUtil hBaseUtil;

    /**
     * 查询es中某个具体索引的全部数据
     * @param indexName
     * @return
     */
    @Override
    public List<Map<String, Object>> getCartDataFromES(String indexName) {
        /**
         * 实现步骤：
         * 1.获取es客户端连接
         * 2.根据索引名称创建查询请求
         * 3.创建SearchSourceBuilder
         * 4.设置查询索引中的全部数据
         * 5.设置从哪儿开始查询
         * 6.设置查询的条数
         * 7.封装searchRequest的source
         * 8.开始查询
         */
        // 1.获取es客户端连接
        RestHighLevelClient client = esClientUtil.getRestHighLevelClient();
        RequestOptions options = RequestOptions.DEFAULT;

        // 2.根据索引名称创建查询请求
        SearchRequest searchRequest = new SearchRequest(indexName);

        // 3.创建SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 4.设置查询索引中的全部数据
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        searchSourceBuilder.query(matchAllQueryBuilder);
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        // 5.设置从哪儿开始查询
        searchSourceBuilder.from(0);

        // 6.设置查询的条数,默认显示10条
        searchSourceBuilder.size(1000);

        // 7.封装searchRequest的source
        SearchRequest request = searchRequest.source(searchSourceBuilder);

        // 8.开始查询
        // 创建存放查询结果的list
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        try {
            SearchResponse response = client.search(request, options);
            for (SearchHit hit : response.getHits().getHits()) {
//                String sourceAsString = hit.getSourceAsString();
//                System.out.println(sourceAsString);
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                System.out.println(sourceAsMap);

                list.add(sourceAsMap);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return list;
    }

    /**
     * 根据关键词查询es中的数据，并返回文档的id(即hbase中对应的rowkey)
     * @param indexName
     * @param keyword
     * @return
     */
    @Override
    public List<String> getCartDataByKeyword(String indexName, String keyword) {
        /**
         * 实现步骤：
         * 1.创建es客户端连接
         * 2.构建查询参数
         * 3.创建查询请求
         * 4.执行查询请求
         */
        // 1.创建es客户端连接
        RestHighLevelClient cleint = esClientUtil.getRestHighLevelClient();

        // 2.构建查询参数
        MultiMatchQueryBuilder multiMatchQueryBuilder = QueryBuilders.multiMatchQuery(keyword, "goodsName", "shopName");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(multiMatchQueryBuilder);
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(100);
        searchSourceBuilder.timeout(new TimeValue(2000, TimeUnit.SECONDS));

        // 3.创建查询请求
        SearchRequest request = new SearchRequest(indexName).source(searchSourceBuilder);

        // 4.执行查询请求
        List<String> idList = new ArrayList<>();
        try {
            SearchResponse response = cleint.search(request, RequestOptions.DEFAULT);
            for (SearchHit hit : response.getHits().getHits()) {
                String id = hit.getId();
                idList.add(id);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return idList;
    }

    @Override
    public List<Object> getCartDataFromHBase(String indexName, String keyword) {
        List<String> rowkeyList = getCartDataByKeyword(indexName, keyword);
        for (String rowkey : rowkeyList) {
            Connection connection = hBaseUtil.getConnection();
            Table table;
            try {
               table = connection.getTable(TableName.valueOf("dwd_itcast_cart"));

               // 创建Get请求
                Get get = new Get(Bytes.toBytes(rowkey));
                Result result = table.get(get);
                for (Cell cell : result.rawCells()) {
                    System.out.println("RK: " + Bytes.toString(CellUtil.cloneRow(cell)) +
                            ", CF: " + Bytes.toString(CellUtil.cloneFamily(cell)) +
                            ", CN: " + Bytes.toString(CellUtil.cloneQualifier(cell)) +
                            ", Value: " + Bytes.toString(CellUtil.cloneValue(cell))
                    );

//                    String field = Bytes.toString(CellUtil.cloneQualifier(cell));
//                    String value = Bytes.toString(CellUtil.cloneValue(cell));
//                    strings.add(field + ": " + value);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
