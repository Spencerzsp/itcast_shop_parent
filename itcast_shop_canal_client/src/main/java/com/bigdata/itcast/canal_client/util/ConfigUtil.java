package com.bigdata.itcast.canal_client.util;

import java.io.IOException;
import java.util.Properties;

/**
 * @ description:工具类读写config.properties配置文件
 * @ author: spencer
 * @ date: 2020/12/9 14:51
 */
public class ConfigUtil {

    private static Properties properties;

    static {
        properties = new Properties();
        try {
            properties.load(ConfigUtil.class.getClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        System.out.println(canaServerIp());
        System.out.println(canalServerPort());
        System.out.println(kafkaValue_serializer_class_config());
    }

    public static String canaServerIp() {
        return properties.getProperty("canal.server.ip");
    }

    public static String canalServerPort() {
        return properties.getProperty("canal.server.port");
    }

    public static String canaServerDestination() {
        return properties.getProperty("canal.server.destination");
    }

    public static String canaServerUsername() {
        return properties.getProperty("canal.server.username");
    }

    public static String canaServerPassword() {
        return properties.getProperty("canal.server.password");
    }

    public static String canaSubscribeFilter() {
        return properties.getProperty("flink.*");
    }

    public static String zookeeperServerIp() {
        return properties.getProperty("zookeeper.server.ip");
    }

    public static String kafkaBootstrap_servers_config() {
        return properties.getProperty("kafka.bootstrap_servers_config");
    }

    public static String kafkaBtach_size_config() {
        return properties.getProperty("kafka.batch_size_config");
    }

    public static String kafkaAcks() {
        return properties.getProperty("kafka.acks");
    }

    public static String kafkaRetries() {
        return properties.getProperty("kafka.retries");
    }

    public static String kafkaClient_id_config() {
        return properties.getProperty("kafka.client_id_config");
    }

    public static String kafkaKey_serializer_class_config() {
        return properties.getProperty("kafka.key_serializer_class_config");
    }

    public static String kafkaValue_serializer_class_config() {
        return properties.getProperty("kafka.value_serializer_class_config");
    }

    public static String kafkaTopic() {
        return properties.getProperty("kafka.topic");
    }
}
