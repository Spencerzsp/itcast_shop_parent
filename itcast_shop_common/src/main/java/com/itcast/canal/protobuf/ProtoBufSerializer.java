package com.itcast.canal.protobuf;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @ description: 实现kafka的value自定义序列化
 *                传递的对象必须实现ProtoBufable序列化接口
 * @ author: spencer
 * @ date: 2020/12/9 16:02
 */
public class ProtoBufSerializer implements Serializer<ProtoBufable> {
    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toBytes();
    }

    @Override
    public void close() {

    }
}
