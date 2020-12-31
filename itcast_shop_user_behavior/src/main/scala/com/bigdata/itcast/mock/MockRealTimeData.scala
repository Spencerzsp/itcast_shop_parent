package com.bigdata.itcast.mock

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockRealTimeData {

  def createKafkaProducer(brokers: String): KafkaProducer[String, String] = {

    val prop = new Properties()
    // 添加配置
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](prop)
  }

  def generateMockDate() = {

    val array = ArrayBuffer[String]()

    val random = new Random()

    for (i <- 100000 until 200000) {
      val userId = random.nextInt(100)
      val itemId = random.nextInt(100)
      val categoryId = random.nextInt(100)

      val behaviors = Array("pv", "buy", "cart", "fav")
      val behavior = behaviors(random.nextInt(4))

      val timestamp = System.currentTimeMillis()

      array += userId + "," + itemId + "," + categoryId + "," + behavior + "," + timestamp
    }

    array.toArray
  }

  def main(args: Array[String]): Unit = {

    val brokers = "wbbigdata00:9092,wbbigdata01:9092,wbbigdata02:9092"
    val topics = "user_behavior"

    val kafkaProducer = createKafkaProducer(brokers)

    while (true){
      for (item <- generateMockDate()) {
        kafkaProducer.send(new ProducerRecord[String, String](topics, item))
//        println("正在发送数据到kafka集群...")
        println(item)
      }

      Thread.sleep(5000)
    }
  }

}
