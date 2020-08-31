package com.zxxj.hotitems_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, Slide, Table}
import org.apache.flink.types.Row

/**
 * @author shkstart
 * @create 2020-08-07 6:54
 */

//case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object HotItemsWithTableApi {
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

    // 将流直接注册成表,并提取需要的字段，定义时间属性
    tableEnv.createTemporaryView("dataTable", dataStream, 'itemId, 'behavior, 'timestamp.rowtime as 'ts)

    val resultTable: Table = tableEnv.sqlQuery(
      """
        |select *
        |from (
        |    select *, row_number() over (partition by windowEnd order by cnt desc) as row_num
        |    from
        |      (
        |         select itemId, hop_end(ts, interval '5' minute , interval '1' hour) as windowEnd , count(itemId) as cnt
        |         from dataTable
        |         where behavior = 'pv'
        |         group by itemId , hop(ts,interval '5' minute , interval '1' hour )
        |      )
        |)
        |where row_num <=5
        |
        |""".stripMargin)

    resultTable.toRetractStream[Row].print("result")
    env.execute(" TOPN  FLINK-SQL")


  }
}
