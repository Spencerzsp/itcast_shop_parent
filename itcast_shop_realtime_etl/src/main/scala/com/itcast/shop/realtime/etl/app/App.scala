package com.itcast.shop.realtime.etl.app

import com.itcast.canal.bean.CanalRowData
import com.itcast.shop.realtime.etl.`trait`.MysqlBaseETL
import com.itcast.shop.realtime.etl.process._
import com.itcast.shop.realtime.etl.utils.GlobalConfigUtil
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * @ description: 创建实时ETL模块的驱动类
  * 实现所有的ETL业务
  * @ author: spencer
  * @ date: 2020/12/10 13:11
  */
object App {

  /**
    * 入口函数
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    /**
      * 实现步骤：
      * 1.初始化flink的流式运行环境
      * 2.设置并行度
      * 3.开启checkpoint
      * 4.接入kafka数据源，消费kafka数据
      * 5.实现所有ETL业务
      * 6.执行任务
      */
    System.setProperty("HADOOP_USER_NAME", "root")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 设置同一个时间，只能有一个检查点
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000))
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置statebackend
    env.setStateBackend(new FsStateBackend("hdfs://wbbigdata00:8020/user/root/checkpoint"))
    // 配置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 设置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // 使用分布式缓存将ip地址资源库数据拷贝到taskmanager节点上
    env.registerCachedFile(GlobalConfigUtil.ip_file_path, "qqwry.dat")

    // TODO 5.实现所有ETL业务
    // 5.1:增量更新维度数据到redis中(全量同步已在DimentionDataLoader中实现)
    val syncDimData = new SyncDimData(env)
    syncDimData.process()

    // 5.2:点击流日志实时的ETL
    // 2001-980:91c0:1:8d31:a232:25e5:85d 218.88.24.59 - [05/Sep/2010:11:27:50 +0200] "GET /images/my.jpg HTTP/1.1" 404 23617 "http://www.angularjs.cn/A00n" "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; N1-N1) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8"
    /**
      * Caused by: java.nio.file.FileSystemException: C:\Users\hadoop\AppData\Local\Temp\blobStore-d3c98348-另一个程序正在使用
      * 解决办法：
      * 1.将日志打印级别设置为INFO，查看具体的异常信息
      * 2.发现没有点击日志输入的时候，仍然在读kafka之前异常的数据，说明该条数据没有被消费，offset没有更新
      * 3.更改自动提交offset为false
      * 4.删除kafka的topic重新测试正常
      */
    val clickLogProcess: ClickLogDataETL = new ClickLogDataETL(env)
    clickLogProcess.process()

    // 5.3.订单数据的实时ETL
    val orderProcess: OrderDataETL = new OrderDataETL(env)
    orderProcess.process()

    // 5.4 订单明细表的实时ETL(异步io查询redis)
    val orderDetailDataProcess = new OderDetailDataETL(env)
    orderDetailDataProcess.process()

    // 5.5商品信息表的实时ETL(异步io查询redis)
    val goodsProcess = new GoodsDataETL(env)
    goodsProcess.process()

    // 5.6购物车数据的实时ETL
    // {"addTime":1608254594005,"count":1,"goodsId":"105789","guid":"f1eeb1d9-9eec-88da-61f87ab0302c","ip":"118.15.100.12","userId":"100208"}
    val cartProcess = new CartDataETL(env)
    cartProcess.process()

    // 5.7评论数据的实时ETL
    // {"comments":"外观不错，音效不错，性价比高，值得购买的一款机器", "timestamp":1608194051, "goodsId":"111059", "assetsVideoJson":"spencer_spark@sina.com", "orderGoodsId":"818654", "startScore":4, "userId":"104371", "userName":"德玛西亚皇子"}
    val commentsProcess = new CommentsDataETL(env)
    commentsProcess.process()

    // TODO 6.执行任务
    env.execute()

  }
}
