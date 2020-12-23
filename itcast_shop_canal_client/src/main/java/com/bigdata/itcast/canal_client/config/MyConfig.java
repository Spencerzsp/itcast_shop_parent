package com.bigdata.itcast.canal_client.config;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/9 17:42
 */
public class MyConfig {

    public static final String CANAL_SERVER_IP = "canal.server.ip";
    public static final String CANAL_SERVER_PORT = "canal.server.port";
    public static final String DESTINATION = "canal.server.destination";
    public static final String CANAL_SUBSCRIBE_FILTER = "canal.subscribe.filter";
    public static final String CANAL_SERVER_USERNAME = "canal.server.username";
    public static final String CANAL_SERVER_PASSWORD = "canal.server.password";

    public static final String ZOOKEEPER_SERVER_IP = "zookeeper.server.ip";

    public static final String KAFKA_BOOTSTRAP_SERVERS_CONFIG = "kafka.bootstrap_servers_config";
    public static final String KAFKA_BATCH_SIZE_CONFIG = "kafka.batch_size_config";
    public static final String KAFKA_ACKS = "kafka.acks";
    public static final String KAFKA_RETRIES = "kafka.retries";
    public static final String KAFKA_CLIENT_ID_CONFIG = "kafka.client_id_config";
    public static final String KAFKA_KEY_SERIA = "kafka.key_serializer_class_config";
    public static final String KAFKA_VALUE_SERIA = "kafka.value_serializer_class_config";
    public static final String KAFKA_TOPIC = "kafka.topic";
}
