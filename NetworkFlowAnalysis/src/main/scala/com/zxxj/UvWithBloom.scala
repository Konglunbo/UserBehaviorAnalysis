package com.zxxj

import com.zxxj.UniqueVisitor.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow

/**
 * @author shkstart
 * @create 2020-05-27 7:06
 */
object UvWithBloom {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2. 读取数据
    // 用相对路径获取数据源
    val resource = getClass.getResource("/UserBehavior.csv")
    env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.behavior == "pv") // 只统计pv操作
      .map(data => ("dummyKey", data.userId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new myTrigger) //创建一个触发器
  }
}

class myTrigger extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
    // 每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }

//定义布隆过滤器
  class Bloom(size: Long) extends Serializable {
    // 位图的总大小，默认16M
    private val cap = if (size > 0) size else 1 << 27

    // 定义hash函数
    def hash(value: String, seed: Int): Long = {
      var result = 0L
      for( i <- 0 until value.length ){
        result = result * seed + value.charAt(i)
      }
      result  & ( cap - 1 )
    }
  }



}