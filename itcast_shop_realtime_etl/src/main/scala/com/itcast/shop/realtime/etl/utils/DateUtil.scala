package com.itcast.shop.realtime.etl.utils

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

/**
  * @ description: 
  * @ author: spencer
  * @ date: 2020/12/14 17:42
  */
object DateUtil {

  /**
    * 将字符串转换为日期类型
    * @param time
    * @return
    */
  // 05/Sep/2010:11:27:50 +0200
  def date2Date(time: String) = {
    val sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
    val date: Date = sdf.parse(time)
    date
  }

  /**
    * 将日期转换为指定的字符串时间
    * @param date
    * @param format
    * @return
    */
  def date2Str(date: Date, format: String) = {
    val sdf = new SimpleDateFormat(format)
    val str: String = sdf.format(date)
    str
  }

  /**
    * 将时间戳转换为指定格式的时间字符串：1608195976 -> 2020-12-17 17:06:16
    * @param timestamp 原时间戳精确到秒
    * @param format
    * @return
    */
  def timestampSeconds2Str(timestamp: Long, format: String) = {
    val date = new Date(timestamp * 1000L)
    val sdf = new SimpleDateFormat(format)
    sdf.format(date)
  }

  /**
    * 将时间戳转换为指定格式的时间字符串，
    * @param timestamp 原时间戳精确到毫秒
    * @param format
    */
  def timestampMillSeconds2str(timestamp: Long, format: String) ={
    val sdf = new SimpleDateFormat(format)
    val date = new Date(timestamp)
    sdf.format(date)
  }

  def main(args: Array[String]): Unit = {

//    val date: Date = date2Date("05/Sep/2010:11:27:50 +0200")
//    println(date)

//    println(date2Str(date, "yyyy-MM-dd HH:mm:ss"))

//    println(longTime2Str(16081940511000L, "yyyy-MM-dd HH:mm:ss"))

//    val date = new Date(1608194051)
//    println(date)
//    val timestamp: Timestamp = new Timestamp(1608194051)
//    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val timestamp: Long = System.currentTimeMillis() / 1000
//
//    val date = new Date(timestamp)
//    val time: String = sdf.format(date)
//
//    println(time)
//
//    println(System.currentTimeMillis() / 1000)
  }

}
