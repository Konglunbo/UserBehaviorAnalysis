package com.zxxj

import java.lang

import com.zxxj.UniqueVisitor.getClass
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

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
    val dataStream: DataStream[UvCount] = env.readTextFile(resource.getPath)
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
      .process(new UvCountWithBloom())
    dataStream.print()
    env.execute("uv with bloom job")
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


}

//定义布隆过滤器
class Bloom(size: Long) extends Serializable {
  // 位图的总大小，默认16M
  private val cap = if (size > 0) size else 1 << 27

  // 定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}

// 定义redis连接
// 位图的存储方式，key是windowEnd，value是bitmap
// 把每个窗口的uv count值也存入名为count的redis表，存放内容为（windowEnd -> uvCount），所以要先从redis中读取
// 用布隆过滤器判断当前用户是否已经存在
// 定义一个标识位，判断reids位图中有没有这一位
// 如果不存在，位图对应位置1，count + 1
// abstract class ProcessWindowFunction[IN, OUT, KEY, W <: Window]
class UvCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  //redis连接
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    // 位图的存储方式，key是windowEnd，value是bitmap
    val storeKey: String = context.window.getEnd.toString
    var count = 0L
    // 把每个窗口的uv count值也存入名为count的redis表，存放内容为（windowEnd -> uvCount），所以要先从redis中读取
    if (jedis.hget("count", storeKey) != null) {
      count = jedis.hget("count", storeKey).toLong

    }
    // 用布隆过滤器判断当前用户是否已经存在
    val userId: String = elements.last._2.toString
    val offset: Long = bloom.hash(userId, 61)
    // 定义一个标识位，判断reids位图中有没有这一位
    val isExsit: lang.Boolean = jedis.getbit(storeKey, offset)
    if (!isExsit) {
      // 如果不存在，位图对应位置1，count + 1
      jedis.setbit(storeKey, offset, true)
      // count+1 存入到redis中
      jedis.hset("count", storeKey, (count + 1).toString)
      out.collect(UvCount(context.window.getEnd.toLong, count + 1))

    } else {
      out.collect(UvCount(storeKey.toLong, count))
    }


  }
}
