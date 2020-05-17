package com.zxxj.hotitems_analysis


import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

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
    dataStream.filter(_.behavior == "pv")
      //根据商品的id 进行聚合
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      //preAggregator: AggregateFunction[T, ACC, V],
      //      windowFunction: WindowFunction[V, R, K, W]): DataStream[R]
      .aggregate(new CountAgg(), new WindowResult()) //自定义窗口函数


    // 控制台输出
    dataStream.print()

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