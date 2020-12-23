package com.bigdata.itcast.canal_client.kafka;

import com.itcast.canal.bean.CanalRowData;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @ description:
 * @ author: spencer
 * @ date: 2020/12/16 11:56
 */
public class KafkaProducerTest {

    Properties properties = null;
    KafkaProducer<String, CanalRowData> kafkaProducer;
    public KafkaProducerTest() {
        properties = new Properties();
        kafkaProducer = new KafkaProducer<String, CanalRowData>(properties);
//        properties
    }

    public void send(CanalRowData canalRowData){
        kafkaProducer.send(new ProducerRecord<String, CanalRowData>("", canalRowData));
    }
}
