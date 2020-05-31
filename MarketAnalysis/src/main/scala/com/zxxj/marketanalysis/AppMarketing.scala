package com.zxxj.marketanalysis

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author shkstart
 * @create 2020-05-31 11:18
 */
object AppMarketing {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val dataStream: DataStream[MarketingViewCount] = env.addSource(new SimulatedEventSource())
      .assignAscendingTimestamps(_.timestamp)
      .filter(_.behavior != "UNINSTALL")
      .map(data => {
        ("dummyKey", 1L)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1), Time.seconds(10))
      .aggregate(new CountAgg(), new MarketingCountTotal())
    dataStream.print()
    env.execute("app marketing total job")
  }
}

// public interface AggregateFunction<IN, ACC, OUT> extends Function, Serializable
class CountAgg() extends AggregateFunction[(String, Long), Long, Long] {
  override def createAccumulator(): Long = 0L

  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

// trait WindowFunction[IN, OUT, KEY, W <: Window] extends Function with Serializable
class MarketingCountTotal() extends WindowFunction[Long, MarketingViewCount, String, TimeWindow] {
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[MarketingViewCount]): Unit = {
    val windowStart: String = new Timestamp(window.getStart).toString
    val windowEnd: String = new Timestamp(window.getEnd).toString
    val count = input.iterator.next()
    out.collect(MarketingViewCount(windowStart, windowEnd, "app marketing", "total", count))
  }
}