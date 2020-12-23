package com.itcast.shop.realtime.etl.utils.pool

import scala.beans.BeanProperty

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/10 11:48
  */
class ConnectionPoolConfig {

  @BeanProperty var maxTotal: Int = _

  @BeanProperty var maxIdle: Int = _

  @BeanProperty var maxWaitMillis: Long = _

  @BeanProperty var testOnBorrow: Boolean = _
}
