package com.zxxj


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * @author shkstart
 * @create 2020-05-27 6:43
 */


//定义输入数据的样例类
// 561558,3611281,965809,pv,1511658000
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

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
    val dataStream = env.readTextFile(resource.getPath)
      .map( data => {
        val dataArray = data.split(",")
        UserBehavior( dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong )
      } )
      .assignAscendingTimestamps(_.timestamp * 1000L)
      .filter( _.behavior == "pv" )    // 只统计pv操作
      .timeWindowAll( Time.hours(1) )
      .apply( new UvCountByWindow() )

    dataStream.print()
    env.execute("uv job")
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, values: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    // 定义一个scala set，用于保存所有的数据userId并去重
    var idSet: Set[Long] = Set[Long]()
    // 把当前窗口所有数据的ID收集到set中，最后输出set的大小
    for (userBehavior <- values) {
      idSet += userBehavior.userId
    }
    out.collect( UvCount( window.getEnd, idSet.size ) )
  }
}