package com.bigdata.itcast.another3;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * @ description: 获取第三方接口数据
 * @ author: spencer
 * @ date: 2020/12/25 15:07
 */
public class GetAnother3Data {

    public static void main(String[] args) throws Exception {
        String interfaceData = getInterfaceData();
//        System.out.println(interfaceData);

        parseInterfaceData(interfaceData);
    }

    /**
     * 获取第三方接口数据
     * @param interfaceData
     */
    public static void parseInterfaceData(String interfaceData) {
        JSONArray jsonArray = JSONArray.parseArray(interfaceData);
        for (Object json : jsonArray) {
            System.out.println(json);
            JSONObject jsonObject = JSONObject.parseObject(json.toString());
            String addTime = jsonObject.getString("addTime");
            String clientCity = jsonObject.getString("clientCity");

            System.out.println(addTime);
            System.out.println(clientCity);

        }
    }

    /**
     * 通过HttpURLConnection获取第三方接口数据
     * @return
     * @throws Exception
     */
    public static String getInterfaceData() throws Exception {
        String strUrl = "http://localhost:8070/cart_es_hbase/dwd_itcast_cart2/%E6%88%B4%E5%B0%94%E5%A4%A9%E8%B5%90%E4%B8%93%E5%8D%96%E5%BA%97";
        URL url = new URL(strUrl);
        HttpURLConnection httpConn = (HttpURLConnection) url.openConnection();
        httpConn.setRequestMethod("GET");
        httpConn.connect();

        BufferedReader reader = new BufferedReader(new InputStreamReader(httpConn.getInputStream()));
        String line;
        StringBuffer buffer = new StringBuffer();
        while ((line = reader.readLine()) != null){
            buffer.append(line);
        }
        reader.close();
        httpConn.disconnect();

        return buffer.toString();
    }
}
