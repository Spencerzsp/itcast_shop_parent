package com.itcast.shop.realtime.etl.utils

import com.itcast.canal.bean.CanalRowData
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

/**
  * @ description: 自定义反序列化实现类，继承AbstractDeserializationSchema抽象类
  * @ author: spencer
  * @ date: 2020/12/10 14:46
  */
class CanalRowDataDeserializerSchema extends AbstractDeserializationSchema[CanalRowData]{

  /**
    * 将kafka读取到的字节码数据转换成CanalRowData对象
    * @param message
    * @return
    */
  override def deserialize(message: Array[Byte]): CanalRowData = {
    new CanalRowData(message)
  }
}
