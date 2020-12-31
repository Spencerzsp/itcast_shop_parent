package com.itcast.canal.util;

import com.alibaba.druid.pool.DruidDataSource;

import java.io.Serializable;

/**
 * @ description: druid连接池工具类
 * @ author: spencer
 * @ date: 2020/12/29 13:16
 */
public class DBDruidUtil implements Serializable {

    private String driver;
    private String url;
    private String userName;
    private String password;

    // 定义DruidDataSource
    DruidDataSource druidDataSource;

    // 定义DruidDataSource的初始化参数
    public final static Integer INIT_SIZE = 20;
    public final static Integer MAX_ACTIVE = 5;
    public final static Integer MIN_IDLE = 5;
    public final static Integer MAX_WAIT = 5 * 10000;
    public final static Integer ABANDED_TIMEOUT = 6;

    // 初始化相关参数
    public DBDruidUtil(String driver, String url, String userName, String password) {
        this.driver = driver;
        this.url = url;
        this.userName = userName;
        this.password = password;
    }

    // 获取druidDataSource

    public DruidDataSource getDruidDataSource() {
        if (druidDataSource == null){
            // 设置获取资源连接参数
            druidDataSource = new DruidDataSource();
            druidDataSource.setDriverClassName(driver);
            druidDataSource.setUrl(url);
            druidDataSource.setUsername(userName);
            druidDataSource.setPassword(password);

            // 设置连接池相关参数
            druidDataSource.setInitialSize(INIT_SIZE);
            druidDataSource.setMaxActive(MAX_ACTIVE);
            druidDataSource.setMinIdle(MIN_IDLE);
            druidDataSource.setMaxWait(MAX_WAIT);

            // 设置是否超时回收
            druidDataSource.setRemoveAbandoned(true);
            druidDataSource.setRemoveAbandonedTimeout(ABANDED_TIMEOUT);
        }
        return druidDataSource;
    }
}
