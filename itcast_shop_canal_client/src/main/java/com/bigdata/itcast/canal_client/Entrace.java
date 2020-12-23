package com.bigdata.itcast.canal_client;

/**
 * @ description: canal客户端的入口类
 * @ author: spencer
 * @ date: 2020/12/9 15:13
 */
public class Entrace {

    public static void main(String[] args) {
        CanalClient canalClient = new CanalClient();
        canalClient.start();
    }
}
