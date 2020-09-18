package com.zxxj.loginfail_detect

import java.{lang, util}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * @author shkstart
 * @create 2020-06-04 6:28
 */
// 需求 ： 两秒中连续登陆失败两次
// 两种方式来确定登陆失败
// 1. 定义ListState，保存2秒内的所有登录失败事件，并设置定时器，触发定时器的时候，根据状态里的失败个数决定是否输出报警
// 2. 每次失败的时候，判断之前是否有登录失败事件，如果已经有登录失败事件，就比较事件时间，如果两次间隔小于2秒，输出报警

// 5402,83.149.11.115,success,1558430815
// 输入的登录事件样例类
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long)

// 输出的异常报警信息样例类
case class Warning(userId: Long, firstFailTime: Long, lastFailTime: Long, warningMsg: String)

object LoginFail {
  def main(args: Array[String]): Unit = {
    // 1. 获取环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    // 2. 读取数据
    val resource = getClass.getResource("/LoginLog.csv")
    val loginEventStream: DataStream[LoginEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
      override def extractTimestamp(element: LoginEvent): Long = element.eventTime * 1000L
    })
    // 自定义processFunction,通过注册定时器实现判断2s内连续登陆失败的需求
    val warningStream: DataStream[Warning] = loginEventStream
      .keyBy(_.userId) // 按照用户id分组检测
      .process(new LoginWarningPro())
    warningStream.print()
    env.execute("login fail detect job")

  }
}

// KeyedProcessFunction<K, I, O> extends AbstractRichFunction
// 自定义Process Function 实现对两次连续登陆失败的实时检测
class LoginWarningPro() extends KeyedProcessFunction[Long, LoginEvent, Warning] {

  // 因为只考虑两次登陆失败，所以只需要保存上一个登陆失败的事件
  // 创建 listState来保存状态，保存2秒内的所有登录失败事件
  lazy val lastloginFailState: ValueState[LoginEvent] = getRuntimeContext.getListState(new ValueStateDescriptor[LoginEvent]("last-login-fail", classOf[LoginEvent]))


  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 获取状态(上次登陆失败事件)
    val loginFailList = lastloginFailState.value()

    // 判断当前事件类型，只处理失败事件，如果是成功，状态直接清空
    if (value.eventType == "fail") {
      //如果是失败，还需要判断之前是否已经有失败事件
      if (lastloginFailState != null) {
        // 如果已经有第一次失败事件，现在是连续失败，要判断时间差是否在两秒以内
        if (value.eventTime - loginFailList.eventTime <= 2) {
          // 如果是两秒之内，输出报警
          out.collect(Warning(value.userId, loginFailList.eventTime, value.eventTime, "login fail"))
        }
      }
      lastloginFailState.update(value)

    } else {
      lastloginFailState.clear()
    }


  }


}
