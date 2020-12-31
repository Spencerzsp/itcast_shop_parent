package com.bigdata.itcast.hotitems

import java.sql.Timestamp
import java.util.Properties

import com.itcast.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
  * @ description: 实时统计时间窗口内的topN PV
  * @ author: spencer
  * @ date: 2020/12/28 16:09
  */
// 定义输入数据的样例类
case class ItemView(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义输出数据的样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

object UVCountAnalysis {

  def main(args: Array[String]): Unit = {
    /**
      * 实现步骤：
      * 1.获取kafka数据源
      * 2.将kafka中的数据转换为样例类所对应的DataStream，并设置watermark
      * 3.过滤出pv的数据
      * 4.将过滤后的数据按照itemId分组
      * 5.划分时间窗口
      * 6.对时间窗口内的数据进行计算
      * 7.根据计算完成的时间窗口分组
      * 8.自定义时间窗口的打印实现
      * 9.真正执行
      */
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1.获取kafka数据源
    val rawDataStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String](
      "user_behavior",
      new SimpleStringSchema(),
      KafkaProps.getKafkaProperties()
    ))

    // 2.将kafka中的数据转换为样例类所对应的DataStream，并设置watermark
    val itemViewCountDataStream: DataStream[ItemView] = rawDataStream.map(value => {
      val dataArray: Array[String] = value.split(",")
      ItemView(
        dataArray(0).toLong,
        dataArray(1).toLong,
        dataArray(2).toInt,
        dataArray(3),
        dataArray(4).toLong
      )
    })
//      .assignAscendingTimestamps(_.timestamp)
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ItemView](Time.seconds(10)) {
      override def extractTimestamp(element: ItemView): Long = element.timestamp
    })

    // 3.过滤出pv的数据
    val itemViewCountPVDataStream: DataStream[ItemView] = itemViewCountDataStream.filter(_.behavior == "pv")

    // 4.将过滤后的数据按照itemId分组
    // 5.划分时间窗口
    // 6.对时间窗口内的数据进行计算
    val countAggPVDataStream: DataStream[ItemViewCount] = itemViewCountPVDataStream.keyBy(_.itemId)
      .timeWindow(Time.minutes(1), Time.seconds(30)) // 每1min划分一个窗口，每隔30s滑动一次
      .aggregate(new AggregateFunction[ItemView, Long, Long]() {
        override def add(value: ItemView, accumulator: Long): Long = accumulator + 1

        override def createAccumulator(): Long = 0L

        override def getResult(accumulator: Long): Long = accumulator

        override def merge(a: Long, b: Long): Long = a + b
      }, new ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
        override def process(itemId: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
          out.collect(ItemViewCount(itemId, context.window.getEnd, elements.iterator.next()))
        }
      })

    // 7.根据计算完成的时间窗口分组
    // 8.自定义时间窗口的打印实现
    val processedDataStream: DataStream[String] = countAggPVDataStream.keyBy(_.windowEnd)
      .process(new KeyedProcessFunction[Long, ItemViewCount, String] {

        // 创建itemState存储itemViewCount
        var itemState: ListState[ItemViewCount] = _

        override def open(parameters: Configuration): Unit = {
          val itemStateDesc = new ListStateDescriptor[ItemViewCount](
            "item_state",
            classOf[ItemViewCount]
          )
          itemState = getRuntimeContext.getListState(itemStateDesc)

        }

        /**
          * 添加新来的数据到state，并设置定时器准备触发
          *
          * @param value
          * @param ctx
          * @param out
          */
        override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
          itemState.add(value)
          ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
        }

        /**
          * 真正触发定时器计算
          *
          * @param timestamp
          * @param ctx
          * @param out
          */
        override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
          // 创建存放从itemState中拿出的数据的List
          var itemViewCounts = new ListBuffer[ItemViewCount]()
          // 从itemState中拿出数据
          import scala.collection.JavaConversions._
          for (item <- itemState.get()) {
            itemViewCounts += item
          }
          // 拿出完时间窗口内的itemState中的数据后清空
          itemState.clear()

          // 取出点击量排名前三的数据
          val sorted3ItemViewCount: ListBuffer[ItemViewCount] = itemViewCounts.sortBy(_.count).reverse.take(3)

          // 自定义控制台输出样式
          val builder = new StringBuilder()

          // 显示时间
          builder.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

          for (i <- sorted3ItemViewCount.indices) {
            builder.append("NO").append(i + 1).append(":")
              .append(" 商品ID=").append(sorted3ItemViewCount(i).itemId)
              .append(" 浏览量=").append(sorted3ItemViewCount(i).count)
              .append("\n")
          }
          builder.append("===============================").append("\n")

          // 控制输出频率
          Thread.sleep(1000)

          // 将封装好的数据返回
          out.collect(builder.toString())

        }

        override def close(): Unit = super.close()
      })
    processedDataStream.print()

    env.execute("UVCountAnalysis")
  }
}
