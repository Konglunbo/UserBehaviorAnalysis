package com.zxxj.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

/**
 * @author shkstart
 * @create 2020-08-07 6:54
 */

//case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object HotItemsWithTableApi1 {
  def main(args: Array[String]): Unit = {
    //1. 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //创建表执行环境
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()
      .inStreamingMode()
      .build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //2.读取数据
    val dataStream = env.readTextFile("E:\\workspace\\workspace_scala\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
      .map(data => {
        val dataArray = data.split(",")
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      //指定eventTime的时间戳
      .assignAscendingTimestamps(_.timestamp * 1000L)

    // 将流转换成表,并提取需要的字段，定义时间属性
    val dataTable: Table = tableEnv.fromDataStream(dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    // 进行开窗聚合操作
    val aggTable: Table = dataTable
      // 过滤pv
      .filter('behavior === "pv")
      // 先开窗口
      .window(Slide over 1.hours every 5.minutes on 'ts as 'sw)
      .groupBy('itemId, 'sw)
      .select('itemId, 'sw.end as 'windowEnd, 'itemId.count as 'cnt)


    // 将聚合结果表注册到表环境中，通过写SQL的方式实现TOP N 的选取
    tableEnv.createTemporaryView("agg", aggTable, 'itemId, 'cnt, 'windowEnd)
    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |    select *, row_number() over (partition by windowEnd order by cnt desc) as row_num
        |    from agg
        |)
        |where row_num <=5
        |
        |""".stripMargin)

    resultTable.toRetractStream[Row].print("result")
    env.execute(" TOPN  TABLE API")


  }
}
