import java.lang.Thread
import java.util.UUID

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.shop.realtime.etl.bean.CartEntity
import com.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.streaming.api.scala._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * @ description: 模拟生成购物车数据
  * {"addTime":1608254594005,"count":1,"goodsId":"105789","guid":"f1eeb1d9-9eec-88da-61f87ab0302c","ip":"118.15.100.12","userId":"100208"}
  * @ author: spencer
  * @ date: 2020/12/23 16:57
  */
object MockCartData {

  def generateData() = {

    val random = new Random()
    val addTime: Long = System.currentTimeMillis()
    val count: Int = random.nextInt(10)
    val goodId = random.nextInt(10000) + 100000
    val guid = UUID.randomUUID().toString
    val ip = "218." + random.nextInt(255) + "." + random.nextInt(100) + ".100.12"
    val userId = random.nextInt(1000) + 100000

    val cartJson = "{\"addTime\":" + addTime + ",\"count\":" + count + ",\"goodsId\":"  + "\"" + goodId + "\"" + ",\"guid\":" + "\"" + guid + "\"" + ",\"ip\":" + "\"" + ip + "\"" + ",\"userId\":" + "\"" + userId + "\"}"
    println(cartJson)
    cartJson

  }

  def main(args: Array[String]): Unit = {

    val producer = new KafkaProducer[String, String](
      KafkaProps.getKafkaProducerProperties()
    )
    while (true){
      producer.send(new ProducerRecord[String, String](
        "ods_itcast_cart",
        generateData()
      ))

      Thread.sleep(2000)
    }
  }
}
