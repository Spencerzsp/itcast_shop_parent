package com.itcast.shop.realtime.etl.bean

import com.itcast.canal.bean.CanalRowData

import scala.beans.BeanProperty

/**
  * @ description: 订单样例类
  * @ author: spencer
  * @ date: 2020/12/15 15:03
  */
case class OrderDBEntity(
                          @BeanProperty orderId: Long,
                          @BeanProperty orderNo: String,
                          @BeanProperty userId: Long,
                          @BeanProperty orderStatus: Int,
                          @BeanProperty goodsMoney: Double,
                          @BeanProperty deliverType: Int,
                          @BeanProperty deliverMoney: Double,
                          @BeanProperty totalMoney: Double,
                          @BeanProperty realTotalMoney: Double,
                          @BeanProperty payType: Int,
                          @BeanProperty isPay: Int,
                          @BeanProperty areaId: Int,
                          @BeanProperty userAddressId: Int,
                          @BeanProperty areaIdPath: String,
                          @BeanProperty userName: String,
                          @BeanProperty userAddress: String,
                          @BeanProperty userPhone: String,
                          @BeanProperty orderScore: Int,
                          @BeanProperty isInvoice: Int,
                          @BeanProperty invoiceClient: String,
                          @BeanProperty orderRemarks: String,
                          @BeanProperty orderSrc: Int,
                          @BeanProperty needPay: Double,
                          @BeanProperty payRand: Int,
                          @BeanProperty orderType: Int,
                          @BeanProperty isRefund: Int,
                          @BeanProperty isAppraise: Int,
                          @BeanProperty cancelReason: Int,
                          @BeanProperty rejectReason: Int,
                          @BeanProperty rejectOtherReason: String,
                          @BeanProperty isClosed: Int,
                          @BeanProperty goodsSearchKeys: String,
                          @BeanProperty orderunique: String,
                          @BeanProperty isFromCart: Int,
                          @BeanProperty receiveTime: String,
                          @BeanProperty deliveryTime: String,
                          @BeanProperty tradeNo: String,
                          @BeanProperty dataFlag: Int,
                          @BeanProperty createTime: String,
                          @BeanProperty settlementId: Int,
                          @BeanProperty commissionFee: Double,
                          @BeanProperty scoreMoney: Double,
                          @BeanProperty useScore: Int,
                          @BeanProperty orderCode: String,
                          @BeanProperty extractJson: String,
                          @BeanProperty orderCodeTargetId: Int,
                          @BeanProperty noticeDeliver: Int,
                          @BeanProperty invoiceJson: String,
                          @BeanProperty lockCashMoney: Double,
                          @BeanProperty payTime: String,
                          @BeanProperty isBatch: Int,
                          @BeanProperty totalPayFee: Int
                        )

/**
  * 创建订单的伴生对象
  */
object OrderDBEntity{
  def apply(rowData: CanalRowData): OrderDBEntity = {
    OrderDBEntity(
      rowData.getColumns.get("orderId").toLong,
      rowData.getColumns.get("orderNo"),
      rowData.getColumns.get("userId").toLong,
      rowData.getColumns.get("orderStatus").toInt,
      rowData.getColumns.get("goodsMoney").toDouble,
      rowData.getColumns.get("deliverType").toInt,
      rowData.getColumns.get("deliverMoney").toDouble,
      rowData.getColumns.get("totalMoney").toDouble,
      rowData.getColumns.get("realTotalMoney").toDouble,
      rowData.getColumns.get("payType").toInt,
      rowData.getColumns.get("isPay").toInt,
      rowData.getColumns.get("areaId").toInt,
      rowData.getColumns.get("userAddressId").toInt,
      rowData.getColumns.get("areaIdPath"),
      rowData.getColumns.get("userName"),
      rowData.getColumns.get("userAddress"),
      rowData.getColumns.get("userPhone"),
      rowData.getColumns.get("orderScore").toInt,
      rowData.getColumns.get("isInvoice").toInt,
      rowData.getColumns.get("invoiceClient"),
      rowData.getColumns.get("orderRemarks"),
      rowData.getColumns.get("orderSrc").toInt,
      rowData.getColumns.get("needPay").toDouble,
      rowData.getColumns.get("payRand").toInt,
      rowData.getColumns.get("orderType").toInt,
      rowData.getColumns.get("isRefund").toInt,
      rowData.getColumns.get("isAppraise").toInt,
      rowData.getColumns.get("cancelReason").toInt,
      rowData.getColumns.get("rejectReason").toInt,
      rowData.getColumns.get("rejectOtherReason"),
      rowData.getColumns.get("isClosed").toInt,
      rowData.getColumns.get("goodsSearchKeys"),
      rowData.getColumns.get("orderunique"),
      rowData.getColumns.get("isFromCart").toInt,
      rowData.getColumns.get("receiveTime"),
      rowData.getColumns.get("deliveryTime"),
      rowData.getColumns.get("tradeNo"),
      rowData.getColumns.get("dataFlag").toInt,
      rowData.getColumns.get("createTime"),
      rowData.getColumns.get("settlementId").toInt,
      rowData.getColumns.get("commissionFee").toDouble,
      rowData.getColumns.get("scoreMoney").toDouble,
      rowData.getColumns.get("useScore").toInt,
      rowData.getColumns.get("orderCode"),
      rowData.getColumns.get("extractJson"),
      rowData.getColumns.get("orderCodeTargetId").toInt,
      rowData.getColumns.get("noticeDeliver").toInt,
      rowData.getColumns.get("invoiceJson"),
      rowData.getColumns.get("lockCashMoney").toDouble,
      rowData.getColumns.get("payTime"),
      rowData.getColumns.get("isBatch").toInt,
      rowData.getColumns.get("totalPayFee").toInt
    )
  }
}