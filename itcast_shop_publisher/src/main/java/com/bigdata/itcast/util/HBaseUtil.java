package com.bigdata.itcast.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/24 17:49
 */
@org.springframework.context.annotation.Configuration
public class HBaseUtil {
    private Configuration config = HBaseConfiguration.create();
    Connection connection = null;
    /**
     * 获取hbase客户端连接
     * @return
     */
    public Connection getConnection(){
        try {
           connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    /**
     * 关闭连接
     */
    public void closeConnection(){
        if (connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
