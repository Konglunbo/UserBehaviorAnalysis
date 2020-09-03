package com.zxxj

import java.lang
import java.net.URL

import org.apache.flink.api.common.functions.{AggregateFunction, MapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

/**
 * @author shkstart
 * @create 2020-05-26 6:53
 */

//定义输入数据的样例类
// 561558,3611281,965809,pv,1511658000
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

// 定义输出数据样例类
case class PvCount(windowEnd: Long, count: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2. 读取数据
    // 用相对路径获取数据源
    val url: URL = getClass.getResource("/UserBehavior.csv")
    val PvCountStream: DataStream[PvCount] = env.readTextFile(url.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计PV操作
      //      .map(data => ("pv", 1L))
      .map(new MyMapper())
      .keyBy(_._1)

      .timeWindow(Time.hours(1))
      //      .sum(1)
      .aggregate(new PvCountAgg(), new PvCountWindowResult())

    // 把每个key对应的 pv count 值 合并
    val resultStream: DataStream[PvCount] = PvCountStream
      .keyBy(_.windowEnd)
      .process(new TotalPvCount())



    resultStream.print("pv count")

    env.execute("page view job")

  }

  // 实现自定义的Mapper
  class MyMapper() extends MapFunction[UserBehavior, (String, Long)] {
    override def map(value: UserBehavior): (String, Long) = {
      (Random.nextString(4), 1L)

    }
  }

  //自定义预聚合函数
  class PvCountAgg() extends AggregateFunction[(String, Long), Long, Long] {


    override def createAccumulator(): Long = 0L

    override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a + b
  }

  // 自定义窗口函数 作为 输出结果的格式化
  class PvCountWindowResult() extends WindowFunction[Long, PvCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
      out.collect(PvCount(window.getEnd, input.head))
    }
  }

  // 自定义的合并各个Key统计结果的ProcessFunction
  class TotalPvCount() extends KeyedProcessFunction[Long, PvCount, PvCount] {


    // 定义一个状态，用来保存当前已有key的count值总计
    lazy val totalCountState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("total-count", classOf[Long]))

    override def processElement(value: PvCount, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#Context, out: Collector[PvCount]): Unit = {

      val currentTotalCount: Long = totalCountState.value()
      totalCountState.update(currentTotalCount + value.count)

      // 注册一个定时器
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
      // 定时器触发时，直接输出当前的totalCount值
      out.collect(PvCount(ctx.getCurrentKey, totalCountState.value()))

      // 清空状态
      totalCountState.clear()
    }
  }

}
