package com.itcast.shop.realtime.etl.process

import java.io.File
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import com.itcast.canal.util.IPSeeker
import com.itcast.shop.realtime.etl.`trait`.MQBaseETL
import com.itcast.shop.realtime.etl.bean.{ClickLogEntity, ClickLogWideEntity}
import com.itcast.shop.realtime.etl.utils.{DateUtil, GlobalConfigUtil}
import nl.basjes.parse.httpdlog.HttpdLoglineParser
import org.apache.commons.io.FileUtils
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

/**
  * @ description: 点击流日志的实时ETL
  * 需要将点击流日志对象转换成拉宽后的点击对象，增加省份、城市和时间字段
  * @ author: spencer
  * @ date: 2020/12/14 14:24
  */
class ClickLogDataETL(env: StreamExecutionEnvironment) extends MQBaseETL(env) {

  /**
    * 将点击流日志字符串转换成点击流对象，使用LogParsing进行解析
    *
    * @param clickLogDataStream
    * @return
    */
  def etl(clickLogDataStream: DataStream[String]) = {
    val clickLogEntityDataStream: DataStream[ClickLogEntity] = clickLogDataStream.map(new RichMapFunction[String, ClickLogEntity] {

      var parser: HttpdLoglineParser[ClickLogEntity] = _

      // 初始化解析器
      override def open(parameters: Configuration): Unit = {
        parser = ClickLogEntity.createClickLogParse()
      }

      // 解析每一行点击流字符串数据为点击流对象
      override def map(clickLog: String): ClickLogEntity = {
        val clickLogEntity: ClickLogEntity = ClickLogEntity(clickLog, parser)
        clickLogEntity
      }
    })

    // 将点击流对象进行拉宽
    val clickLogWideDataStream: DataStream[ClickLogWideEntity] = clickLogEntityDataStream.map(new RichMapFunction[ClickLogEntity, ClickLogWideEntity] {

      // 定义ip获取省份城市信息的实例对象
      var ipSeeker: IPSeeker = _

      /**
        * 初始化操作，读取分布式缓存文件
        *
        * @param parameters
        */
      override def open(parameters: Configuration): Unit = {

        val file: File = getRuntimeContext.getDistributedCache.getFile("qqwry.dat")
        ipSeeker = new IPSeeker(file)

      }

      override def map(clickLogEntity: ClickLogEntity): ClickLogWideEntity = {
        val clickLogWideEntity = ClickLogWideEntity(clickLogEntity)

        // 根据IP地址获取省份、城市信息
        val country: String = ipSeeker.getCountry(clickLogWideEntity.ip)
        //        println(country)

        val areaArray: Array[String] = country.split("省")
        if (areaArray.size > 1) {
          // 非直辖市
          clickLogWideEntity.province = areaArray(0) + "省"
          clickLogWideEntity.city = areaArray(1)
        } else {
          // 直辖市
          val areaArray: Array[String] = country.split("市")
          if (areaArray.size > 1) {
            clickLogWideEntity.province = areaArray(0) + "市"
            clickLogWideEntity.city = areaArray(1)
          } else {
            clickLogWideEntity.province = areaArray(0) + "市"
            clickLogWideEntity.city = ""
          }
        }

        val date: Date = DateUtil.date2Date(clickLogWideEntity.getRequestTime)
        clickLogWideEntity.requetDateTime = DateUtil.date2Str(date, "yyyy-MM-dd HH:mm:ss")

        clickLogWideEntity
      }
    })

    // 返回拉宽后的点击流对象
    clickLogWideDataStream
  }

  /**
    * 根据业务抽取出来的process方法，所有的ETL都有操作方法
    */
  override def process(): Unit = {
    /**
      * 实现步骤：
      * 1.获取点击流日志的数据源
      * 2.将ngnix的点击流日志字符串转换成点击流对象
      * 3.对点击流对象进行实时拉宽操作，返回拉宽后的点击流实体对象
      * 4.将拉宽后点击流实体类转换成json字符串(方便写入kafka集群)
      * 5.将json字符串写入到kafka集群，供druid进行实时的摄取操作
      */

    // 1.获取点击流日志的数据源
    val clickLogDataStream: DataStream[String] = getKafkaDataStream(GlobalConfigUtil.input_topic_click_log)

    // 2.将ngnix的点击流日志字符串转换成拉宽后的点击流对象
    val clickLogWideEntityDataStream: DataStream[ClickLogWideEntity] = etl(clickLogDataStream)

    clickLogWideEntityDataStream.print("拉宽后的点击流数据：")

    // 2.将拉宽后点击流实体类转换成json字符串(方便写入kafka集群)
    val clickLogJsonDataStream: DataStream[String] = clickLogWideEntityDataStream.map(clickLogWideEntity => {
      JSON.toJSONString(clickLogWideEntity, SerializerFeature.DisableCircularReferenceDetect)
    })

    // 3.将json字符串写入到kafka集群，工druid进行实时的摄取操作
    clickLogJsonDataStream.addSink(kafkaProducer(GlobalConfigUtil.output_topic_clicklog))
  }
}
