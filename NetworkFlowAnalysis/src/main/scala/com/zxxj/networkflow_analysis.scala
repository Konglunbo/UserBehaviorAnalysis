package com.zxxj


import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.Map

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * @author shkstart
 * @create 2020-05-24 15:31
 *
 */

//83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
//输入数据样例类
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)

// 窗口聚合结果样例类
case class UrlViewCount(url: String, windowEnd: Long, count: Long)


object networkflow_analysis {
  // 1.创建执行环境
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
  //2.读取数据
  val dataStream: DataStream[ApacheLogEvent] = env.readTextFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\NetworkFlowAnalysis\\src\\main\\resources\\apache.log")
    .map(data => {
      val dataArray: Array[String] = data.split(" ")
      // 对时间进行转换
      val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
      // 转换成时间戳格式
      val timestamp: Long = simpleDateFormat.parse(dataArray(3).trim).getTime
      // 封装到样例类中
      ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)

    })
    // 此处对延迟数据的处理进行优化，对watermark的延迟时间进行减小，同时在开窗后，允许迟到数据，这样的话就可以先输出结果，在等待的时间内，来一条迟到的数据，就更新一次结果
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
      // 由于前边对eventTime已做时间格式的转换，现在的eventTime已经是 毫秒值，因此不用再次乘以1000L
      override def extractTimestamp(element: ApacheLogEvent): Long = element.eventTime
    })
  // 对数据进行转换操作
  dataStream.keyBy(_.url)
    .timeWindow(Time.minutes(10), Time.seconds(5))
    //preAggregator: AggregateFunction[T, ACC, V],
    //      windowFunction: WindowFunction[V, R, K, W]): DataStream[R]
    // 每个窗口做预聚合之后的结果
    .allowedLateness(Time.minutes(1))
    .sideOutputLateData(new OutputTag[ApacheLogEvent]("late"))
    .aggregate(new CountAgg(), new WindowResult()) //自定义窗口函数
    .keyBy(_.windowEnd)
    .process(new TopNHotUrls(5))


}

// 自定义预聚合 preAggregator: AggregateFunction<IN, ACC, OUT>  ACC 中间的状态
// 该预聚合只是进行条数的统计
class CountAgg() extends AggregateFunction[ApacheLogEvent, Long, Long] {

  override def createAccumulator(): Long = 0L

  override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// 自定义窗口函数，输出ItemViewCount
// WindowFunction[IN, OUT, KEY, W <: Window] extends Function

class WindowResult() extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next()))
  }
}


// 自定义处理函数
// public abstract class KeyedProcessFunction<K, I, O>

// 自定义排序输出处理函数
class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String]{


  // 改进：定义mapstate，用来保存当前窗口所有page的count值，有更新操作时，直接put
  lazy val urlState: MapState[String, Long] = getRuntimeContext.getMapState( new MapStateDescriptor[String, Long]("url-state", classOf[String], classOf[Long] ) )

  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
    // 类似upsert操作
    urlState.put(value.url, value.count)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    // 定义1分钟后的定时器，用于清除状态
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 60 *1000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    // 判断时间戳，如果是1分钟后的定时器，直接清空状态
    if (timestamp == ctx.getCurrentKey + 60 * 1000L){
      urlState.clear()
      return
    }


    // 从状态中拿到数据
    val allUrlViews: ListBuffer[(String, Long)] = new ListBuffer[(String, Long)]()
    // 获取到所有的键值对
    val iter = urlState.entries().iterator()
    while(iter.hasNext){
      val entry = iter.next()
      allUrlViews += (( entry.getKey, entry.getValue ))
    }


     val sortedUrlViews= allUrlViews.sortWith(_._2> _._2).take(topSize)

    // 格式化结果输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append( new Timestamp( timestamp - 1 ) ).append("\n")
    for( i <- sortedUrlViews.indices ){
      val currentUrlView = sortedUrlViews(i)
      result.append("NO").append(i + 1).append(":")
        .append(" URL=").append(currentUrlView._1)
        .append(" 访问量=").append(currentUrlView._2).append("\n")
    }
    result.append("=============================")
    Thread.sleep(1000)
    out.collect(result.toString())

  }
}