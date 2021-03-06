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
      .process(new LoginWarning(2))
    warningStream.print()
    env.execute("login fail detect job")

  }
}

// KeyedProcessFunction<K, I, O> extends AbstractRichFunction
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 创建 listState来保存状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  // 保存注册的定时器时间戳
  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 获取状态
    val loginFailList: lang.Iterable[LoginEvent] = loginFailState.get()
    // 每来一条数据，判断当前登陆事件是成功还是失败
    // 判断状态是否是fail
    if (value.eventType == "fail") {
      // 如果是fail，再判断是否是第一个登陆失败事件，如果是的话，注册定时器，并存在状态中
      loginFailState.add(value)
      if (timerTsState.value() == 0) {
        // 如果没有定时器，就注册一个2s后的定时器
        val ts: Long = value.eventTime * 1000L + 2000L
        ctx.timerService().registerEventTimeTimer(ts)
        timerTsState.update(ts)
      }

    } else {
      // 如果状态成功，删除定时器，则清空状态
      ctx.timerService().deleteEventTimeTimer(timerTsState.value())
      timerTsState.clear()
      loginFailState.clear()
    }


  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
    // 触发定时器的时候，根据状态里的失败个数决定是否输出报警
    // 定时器触发时，说明没有成功事件到来，统计所有的失败事件个数，如果大于设定值就报警
    // 第一种方式
    import scala.collection.JavaConversions._
    val loginFailList: List[LoginEvent] = loginFailState.get().toList
    if (loginFailList.length >= maxFailTimes) {
      out.collect(Warning(loginFailList.head.userId, loginFailList.head.eventTime, loginFailList.last.eventTime, "login fail in 2 seconds for " + loginFailList.length + " times."))
    }

    // 第二种方式
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
    val iter = loginFailState.get().iterator()
    while (iter.hasNext) {
      allLoginFails += iter.next()
    }

    // 判断个数
    if (allLoginFails.length >= maxFailTimes) {
      out.collect(Warning(allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times."))
    }
    // 清空状态
    loginFailState.clear()
    timerTsState.clear()

  }
}
