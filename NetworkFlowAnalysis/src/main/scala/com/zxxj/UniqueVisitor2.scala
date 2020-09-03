package com.zxxj

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random


/**
 * @author shkstart
 * @create 2020-05-27 6:43
 */


//定义输入数据的样例类
// 561558,3611281,965809,pv,1511658000
//case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

case class UvCount(windowEnd: Long, uvCount: Long)

object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2. 读取数据
    // 用相对路径获取数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    val dataStream: DataStream[UserBehavior] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)


    // 进行增量聚合的方式
    val resultStream: DataStream[UvCount] = dataStream
      .filter(_.behavior == "pv")
      .map(data => (Random.nextString(10), data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new UvCountAgg(), new UvCountWindowResult())


    resultStream.print()
    env.execute("uv job")
  }

  //自定义增量聚合函数
  class UvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  class UvCountWindowResult() extends WindowFunction[Long, UvCount, String, TimeWindow] {

    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UvCount]): Unit = {
      out.collect(UvCount(window.getEnd, input.head))
    }
  }

}

