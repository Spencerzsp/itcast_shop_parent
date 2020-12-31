package com.bigdata.itcast.druid;

import java.sql.*;
import java.util.Properties;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/31 13:21
 */
public class DruidJDBCDemo {

    public static void main(String[] args) throws Exception {
        /**
         * 实现步骤：
         * 1.加载Druid的JDBC驱动
         * 2.获取Druid的JDBC连接方式
         * 3.创建pstmt
         * 4.执行sql查询
         * 5.遍历查询结果
         * 6.关闭连接
         */
        // 1.加载Druid的JDBC驱动
        Class.forName("org.apache.calcite.avatica.remote.Driver");

        // 2.获取Druid的JDBC连接方式
        Connection connection = DriverManager.getConnection("jdbc:avatica:remote:url=http://wbbigdata02:8889/druid/v2/sql/avatica/", new Properties());

        // 3.创建statement
        String sql = "select * from dwd_carts";
        // 不支持使用pstmt，否则报错org.apache.calcite.avatica.com.fasterxml.jackson.core.JsonParseException
//        PreparedStatement pstmt = connection.prepareStatement(sql);
        Statement statement = connection.createStatement();

        // 4.执行sql查询
        ResultSet resultSet = statement.executeQuery(sql);

        // 5.遍历查询结果
        while (resultSet.next()){
//            String user = resultSet.getString("user");
//            String url = resultSet.getString("url");

            String goodsName = resultSet.getString("goodsName");

            System.out.println(goodsName);
        }

        // 6.关闭连接
        resultSet.close();
        statement.close();
        connection.close();

    }

}
