package com.bigdata.itcast.util;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @ description: 获取es客户端连接的工具类
 * @ author: spencer
 * @ date: 2020/12/24 15:11
 */
@Configuration
public class ESClientUtil {

    private static RestHighLevelClient restHighLevelClient;

    /**
     * 获取es客户点连接
     * @return
     */
    public static RestHighLevelClient getRestHighLevelClient(){
//        ParameterTool param = FlinkParamToolUtil.getParam();
        restHighLevelClient = new RestHighLevelClient(
                RestClient.builder(new HttpHost(
                        "wbbigdata00",
                        9200,
                        "http"))
        );
        return restHighLevelClient;
    }

    /**
     * 关闭es客户端连接
     */
    public static void closeESClient(){
        if (restHighLevelClient != null){
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(getRestHighLevelClient());

        RestHighLevelClient client = getRestHighLevelClient();

        // 2.根据索引名称创建查询请求
        SearchRequest searchRequest = new SearchRequest("dwd_itcast_cart");

        // 3.创建SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        // 4.设置查询索引中的全部数据
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
//        searchSourceBuilder.query(matchAllQueryBuilder);
//        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));

        SearchRequest request = searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(request, RequestOptions.DEFAULT);
        for (SearchHit fields : response.getHits()) {
            Map<String, Object> sourceAsMap = fields.getSourceAsMap();
            for (String key : sourceAsMap.keySet()) {
                System.out.println(key);
            }
        }

    }
}
