package com.zxxj

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author shkstart
 * @create 2020-05-26 6:53
 */

//定义输入数据的样例类
// 561558,3611281,965809,pv,1511658000
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2. 读取数据
    // 用相对路径获取数据源
    val url: URL = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[(String, Int)] = env.readTextFile(url.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计PV操作
      .map(data => ("pv", 1))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)
    dataStream.print("pv count")

    env.execute("page view job")

  }

}
