package com.zxxj.hotitems_analysis


import java.sql.Timestamp

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1, Tuple2}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author shkstart
 * @create 2020-05-02 15:20
 */

//定义输入数据的样例类
// 561558,3611281,965809,pv,1511658000
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

//定义窗口聚合结果样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //2.读取数据
    val dataStream = env.readTextFile("D:\\IdeaProjects\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      //指定eventTime的时间戳
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 3. transform 处理数据
    val processedStream: DataStream[String] = dataStream.filter(_.behavior == "pv")
      //根据商品的id 进行聚合
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //preAggregator: AggregateFunction[T, ACC, V],
      //      windowFunction: WindowFunction[V, R, K, W]): DataStream[R]
      // 每个窗口做预聚合之后的结果
      .aggregate(new CountAgg(), new WindowResult()) //自定义窗口函数
      // 按照窗口分组
      .keyBy(_.windowEnd)
      .process(new TopNHotItem(3))



    // 控制台输出
    processedStream.print()

    env.execute("hot items job")
  }


}


// 自定义预聚合 preAggregator: AggregateFunction[T, ACC, V]  ACC 中间的状态
// 该预聚合只是进行条数的统计

class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {
  // 创建累加器时，初始值
  override def createAccumulator(): Long = 0L

  // 对新来的一条数据如何处理？
  override def add(in: UserBehavior, acc: Long): Long = {
    acc + 1
  }

  // 获取结果时，如何处理
  override def getResult(acc: Long): Long = {
    acc
  }

  //// 如何进行多分区的Merge
  override def merge(acc: Long, acc1: Long): Long = {
    acc + acc1
  }
}

// 自定义预聚合函数，来求取平均值 中间状态 ACC 用元组的形式，一个存和值，一个存个数
class averageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double] {

  override def createAccumulator(): (Long, Int) = (0L, 0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = {
    (acc._1 + in.timestamp, acc._2 + 1)
  }

  override def getResult(acc: (Long, Int)): Double = {
    acc._1 / acc._2
  }

  override def merge(a: (Long, Int), b: (Long, Int)): (Long, Int) = (a._1 + b._1, a._2 + b._2)
}

// 自定义窗口函数，输出ItemViewCount
// WindowFunction[IN, OUT, KEY, W <: Window] extends Function
// ItemViewCount(itemId: Long, windowEnd: Long, count: Long)
class WindowResult() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next()))
  }
}

// 自定义处理函数
// public abstract class KeyedProcessFunction<K, I, O>
// 需要一个变量 ListState 用来保存一个窗口内所有元素的状态
// 重写 open方法，通过open方法获取 itermState
// processElement 方法里 主要是将数据加入到状态列表，并注册定时器
// 重写onTimer 来指定 定时器触发的时候，对数据的操作
class TopNHotItem(topSize: Int) extends KeyedProcessFunction[Long, ItemViewCount, String] {

  private var itermState: ListState[ItemViewCount] = _


  override def open(parameters: Configuration): Unit = {
    itermState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-state", classOf[ItemViewCount]))
  }

  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
    itermState.add(value)
    // 注册定时器
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 重写onTimer 来指定 定时器触发的时候，对数据的操作
  // 定义list Buffer 将所有state中的数据取出，放入ListBuffer allItems里
  // 按照count大小排序，并取前N个
  // 清空状态
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._
    for (item <- itermState.get()) {
      allItems += item
    }
    // Ordering.Long.reverse 进行降序排列
    val sortedItem = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    // 清空状态
    itermState.clear()
    // 将排名结果格式化输出
    val result: StringBuilder = new StringBuilder()
    result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")
    // 输出每个商品的商品信息
    for (i <- sortedItem.indices) {
      val currentItem: ItemViewCount = sortedItem(i)
      result.append("NO:").append(i + 1).append(":")
        .append("商品ID：").append(currentItem.itemId)
        .append("点击次数：").append(currentItem.count)
        .append("\n")
    }
    result.append("==============================")
    // 控制输出频率
    Thread.sleep(1000)
    out.collect(result.toString())


  }
}