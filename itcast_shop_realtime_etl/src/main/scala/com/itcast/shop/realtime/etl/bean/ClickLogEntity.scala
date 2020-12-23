package com.itcast.shop.realtime.etl.bean

import nl.basjes.parse.httpdlog.HttpdLoglineParser

import scala.beans.BeanProperty

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/14 13:24
  */
class ClickLogEntity {

  // 连接用户
  private[this] var _connectionClientUser: String = _
  def setConnectionClientUser(value: String) = _connectionClientUser = value
  def getConnectionClientUser = _connectionClientUser

  // ip地址
  private[this] var _ip: String = _
  def setIp(value: String) = _ip = value
  def getIp = _ip

  // 请求时间
  private [this] var _requestTime: String = _
  def setRequestTime(value: String) = _requestTime = value
  def getRequestTime = _requestTime

  // 请求方式
  private [this] var _method: String = _
  def setMethod(value:String) = _method = value
  def getMethod = _method

  // 请求资源
  private [this] var _resolution: String = _
  def setResolution(value: String) = _resolution = value
  def getResolution = _resolution

  // 请求协议
  private [this] var _requestProtocol: String = _
  def setRequestProtocol(value: String) = _requestProtocol = value
  def getRequestProtocol = _requestProtocol

  // 请求码
//  private [this] var _requestStatus: Int = _
//  def setRequestStatus(value: Int) = _requestStatus = value
//  def getRequestStatus = _requestStatus

  // 响应码
  private [this] var _responseStaus: String = _
  def setRequestStatus(value: String) = _responseStaus = value
  def getRequestStatus = _responseStaus

  // 返回的数据流量
//  private [this] var _responseBodyBytes: String = _
//  def setResponseBodyBytes(value: String) = _responseBodyBytes = value
//  def getResponseBodyBytes = _responseBodyBytes

  // 访客的来源url
  private [this] var _referer: String = _
  def setReferer(value: String) = _referer = value
  def getReferer = _referer

  // 客户端代理信息
  private [this] var _userAgent: String = _
  def setUserAgent(value: String) = _userAgent = value
  def getUserAgent = _userAgent

  // 跳转过来页面的域名
  private [this] var _referDomain: String = _
  def setReferDomain(value: String) = _referDomain = value
  def getReferDomain = _referDomain

}

// 传递点击流日志解析出来赋值给scala类
object ClickLogEntity{
  // 1.定义点击流日志的解析规则
  // 2001-980:91c0:1:8d31:a232:25e5:85d 218.88.24.59 - [05/Sep/2010:11:27:50 +0200] "GET /images/my.jpg HTTP/1.1" 404 23617 "http://www.angularjs.cn/A00n" "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; N1-N1) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8"
  val logFormat:String = "%u %h %l %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\""


  /**
    * 传递解析器和点击日志生成样例类
    * @param clickLog
    * @param parser
    * @return
    */
  def apply(clickLog: String, parser: HttpdLoglineParser[ClickLogEntity]): ClickLogEntity = {
    val clickLogEntity = new ClickLogEntity()
    parser.parse(clickLogEntity, clickLog)
  }
  // 2.创建解析器
  def createClickLogParse() = {
    // 创建解析器
    val parser: HttpdLoglineParser[ClickLogEntity] = new HttpdLoglineParser[ClickLogEntity](classOf[ClickLogEntity], logFormat)
    // 建立对象的方法名和参数名的映射关系
    parser.addParseTarget("setConnectionClientUser", "STRING:connection.client.user")
    parser.addParseTarget("setIp", "IP:connection.client.host")
    parser.addParseTarget("setRequestTime", "TIME.STAMP:request.receive.time.last")
    parser.addParseTarget("setMethod", "HTTP.METHOD:request.firstline.method")
    parser.addParseTarget("setResolution", "HTTP.URI:request.firstline.uri")
    parser.addParseTarget("setRequestProtocol", "HTTP.PROTOCOL:request.firstline.protocol")
    parser.addParseTarget("setRequestStatus", "STRING:request.status.last")
//    parser.addParseTarget("setResponseBodyBytes", "BYTES:response.body.bytes")
    parser.addParseTarget("setReferer", "HTTP.URI:request.referer")
    parser.addParseTarget("setUserAgent", "HTTP.USERAGENT:request.user-agent")
    parser.addParseTarget("setReferDomain", "HTTP.HOST:request.referer.host")

    parser
  }

  def main(args: Array[String]): Unit = {
    /**
      * %v	进行服务的服务器的标准名字 ServerName，通常用于虚拟主机的日志记录中。
        %h	客户机的 IP 地址。
        %l	从identd服务器中获取远程登录名称，基本已废弃。
        %u	来自于认证的远程用户。
        %t	连接的日期和时间。
        %r	HTTP请求的首行信息，典型格式是“METHOD RESOURCEPROTOCOL”，即“方法 资源 协议”。经常可能出现的 METHOD 是 GET、POST 和 HEAD；RESOURCE 是指浏览者向服务器请求的文档或 URL；PROTOCOL 通常是HTTP，后面再加上版本号，通常是 HTTP/1.1。
        %>s	响应请求的状态代码，一般这项的值是 200，表示服务器已经成功地响应浏览器的请求，一切正常；以 3 开头的状态代码表示由于各种不同的原因用户请求被重定向到了其他位置；以 4 开头的状态代码表示客户端存在某种错误；以 5 开头的状态代码表示服务器遇到了某个错误。
        %b	传送的字节数（不包含HTTP头信息），将日志记录中的这些值加起来就可以得知服务器在一天、一周或者一月内发送了多少数据。
        %{Referer}i	指明了该请求是从被哪个网页提交过来的。
        %U	请求的URL路径，不包含查询串。
        %{User-Agent}i	此项是客户浏览器提供的浏览器识别信息。
      */
    // 定义点击流日志的数据样本
    val logline = "2001-980:91c0:1:8d31:a232:25e5:85d 222.68.172.190 - [05/Sep/2010:11:27:50 +0200] \"GET /images/my.jpg HTTP/1.1\" 404 23617 \"http://www.angularjs.cn/A00n\" \"Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; N1-N1) AppleWebKit/533.17.8 (KHTML, like Gecko) Version/5.0.1 Safari/533.17.8\"";

    val clickLogEntity = new ClickLogEntity()
    val parser: HttpdLoglineParser[ClickLogEntity] = createClickLogParse()
    parser.parse(clickLogEntity, logline)
    println(clickLogEntity.getConnectionClientUser)
    println(clickLogEntity.getIp)
    println(clickLogEntity.getRequestTime)
    println(clickLogEntity.getMethod)
    println(clickLogEntity.getResolution)
    println(clickLogEntity.getRequestProtocol)
    println(clickLogEntity.getRequestStatus)
    println(clickLogEntity.getReferer)
    println(clickLogEntity.getUserAgent)
    println(clickLogEntity.getReferDomain)

  }
}

/**
  *
  * @param uid
  * @param ip
  * @param requestTime
  * @param resuestMethod
  * @param requestUrl
  * @param requestProtocol
  * @param requestStatus
//  * @param requestBodyBytes
  * @param referer
  * @param userAgent
  * @param refererDomain
  * @param province
  * @param city
  * @param requetDateTime
  */
// 定义拉宽后的点击流日志对象
case class ClickLogWideEntity(
                               @BeanProperty uid: String,
                               @BeanProperty ip: String,
                               @BeanProperty requestTime: String,
                               @BeanProperty resuestMethod: String,
                               @BeanProperty requestUrl: String,
                               @BeanProperty requestProtocol: String,
                               @BeanProperty requestStatus: String,
//                               @BeanProperty requestBodyBytes: String,
                               @BeanProperty referer:String,
                               @BeanProperty userAgent: String,
                               @BeanProperty refererDomain: String,
                               @BeanProperty var province: String,
                               @BeanProperty var city: String,
                               @BeanProperty var requetDateTime: String
                             )
object ClickLogWideEntity{
  def apply(clickLogEntity: ClickLogEntity): ClickLogWideEntity = {

    ClickLogWideEntity(
      clickLogEntity.getConnectionClientUser,
      clickLogEntity.getIp,
      clickLogEntity.getRequestTime,
      clickLogEntity.getMethod,
      clickLogEntity.getResolution,
      clickLogEntity.getRequestProtocol,
      clickLogEntity.getRequestStatus,
//      clickLogEntity.getResponseBodyBytes,
      clickLogEntity.getReferer,
      clickLogEntity.getUserAgent,
      clickLogEntity.getReferDomain,
      "",
      "",
      ""
    )
  }
}