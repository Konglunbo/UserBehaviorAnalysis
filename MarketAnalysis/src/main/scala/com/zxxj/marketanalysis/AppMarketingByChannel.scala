package com.zxxj.marketanalysis

import java.sql.Timestamp
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, RichSourceFunction, SourceFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.util.Random

// 输入数据样例类
case class MarketingUserBehavior(userId: String, behavior: String, channel: String, timestamp: Long)

// 输出结果样例类
case class MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)




/**
 * @author shkstart
 * @create 2020-05-31 9:11
 */
object AppMarketingByChannel {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 添加数据源
    // 指定时间戳
    // 对卸载的行为进行过滤
    // 对数据进行转化，准备对 渠道和用户行为做聚合
    // 进行分组
    // 开窗 开窗一小时，滑动10s
    // 自定义处理函数，输出格式化后的样例类
    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource)
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ((data.channel, data.behavior), 1L)
      })
      // 以（channel,behavior） 二元组作为分组的key
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      // 此处使用全窗口作为演示，实际使用中采用增量聚合效率更高
      .process(new MarketingCountByChannel())
    dataStream.print()
    env.execute("app marketing by channel job")

  }
}



// 自定义数据源
class SimulatedEventSource() extends RichSourceFunction[MarketingUserBehavior] {
  // 定义是否运行的标识位
  var running = true
  // 定义用户行为的集合
  val behaviorTypes: Seq[String] = Seq("CLICK", "DOWNLOAD", "INSTALL", "UNINSTALL")
  // 定义渠道的集合
  val channelSets: Seq[String] = Seq("wechat", "weibo", "appstore", "huaweistore")
  // 定义一个随机数发生器
  val rand: Random = new Random()

  override def cancel(): Unit = running = false

  override def run(ctx: SourceFunction.SourceContext[MarketingUserBehavior]): Unit = {
    // 定义一个生成数据的上限
    val maxElements = Long.MaxValue
    var count = 0L

    // 随机生成所有数据
    while (running && count < maxElements) {
      val id = UUID.randomUUID().toString
      val behavior = behaviorTypes(rand.nextInt(behaviorTypes.size))
      val channel = channelSets(rand.nextInt(channelSets.size))
      val ts = System.currentTimeMillis()

      // 使用CTX 发出数据
      ctx.collect(MarketingUserBehavior(id, behavior, channel, ts))

      count += 1
      // 造成数据的间隔
      TimeUnit.MILLISECONDS.sleep(10L)
    }
  }
}

// abstract class ProcessWindowFunction[IN, OUT, KEY, W <: Window]
// MarketingViewCount(windowStart: String, windowEnd: String, channel: String, behavior: String, count: Long)
class MarketingCountByChannel() extends ProcessWindowFunction[((String, String), Long), MarketingViewCount, (String, String), TimeWindow] {
  override def process(key: (String, String), context: Context, elements: Iterable[((String, String), Long)], out: Collector[MarketingViewCount]): Unit = {

    val windowStart: String = new Timestamp(context.window.getStart).toString
    val windowEnd: String = new Timestamp(context.window.getEnd).toString
    val channel: String = key._1
    val behavior = key._2
    val count = elements.size
    out.collect(MarketingViewCount(windowStart, windowEnd, "app marketing", "total", count))
  }
}