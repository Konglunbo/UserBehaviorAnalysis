package com.zxxj.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author shkstart
 * @create 2020-06-11 6:24
 */
object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 1. 读取事件数据，创建简单事件流
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
      })
      .keyBy(_.userId)

    // 2.定义匹配模式
    val loginFailPattern: Pattern[LoginEvent, LoginEvent]
    = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail") // 第一个登陆失败事件
      .next("next").where(_.eventType == "fail") // 紧跟着第二次登陆失败事件
      .within(Time.seconds(5)) // 在2s内连续发生有效


    // 循环模式定义示例(宽松近邻)
    val loginFailPattern2: Pattern[LoginEvent, LoginEvent] = Pattern.begin[LoginEvent]("fails").consecutive() // 更改为严格匹配
      .times(2)
      .where(_.eventType == "fail")
      .within(Time.seconds(10))
    loginFailPattern2


    //3.在事件流上应用模式，得到一个pattern stream
    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream, loginFailPattern)

    // 4. 从pattern stream上应用select function，检出匹配事件序列
    val loginFailDataStream: DataStream[Warning] = patternStream.select(new LoginFailMatch())

    loginFailDataStream.print()

    env.execute("login fail with cep job")

  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Warning] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Warning = {
    // 从map中按照名称取出对应的事件
    val firstFail: LoginEvent = map.get("begin").iterator().next()
    val lastFail: LoginEvent = map.get("next").iterator().next()
    Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!")


  }

  class LoginFailMatch2() extends PatternSelectFunction[LoginEvent, Warning] {
    override def select(pattern: util.Map[String, util.List[LoginEvent]]): Warning = {
      // 从map中按照名称取出对应的事件
      val firstFail: LoginEvent = pattern.get("fails").get(0)
      val lastFail: LoginEvent = pattern.get("fails").get(1)
      Warning(firstFail.userId, firstFail.eventTime, lastFail.eventTime, "login fail!")

    }
  }

}