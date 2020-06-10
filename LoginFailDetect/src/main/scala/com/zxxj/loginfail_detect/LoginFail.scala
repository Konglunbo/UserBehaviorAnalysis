package com.zxxj.loginfail_detect

import java.{lang, util}

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
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
    val warningStream: DataStream[Warning] = loginEventStream
      .keyBy(_.userId)
      .process(new LoginWarning(2))
    warningStream.print()
    env.execute("login fail detect job")

  }
}

// KeyedProcessFunction<K, I, O> extends AbstractRichFunction
class LoginWarning(maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, Warning] {
  // 创建 listState来保存状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-fail-state", classOf[LoginEvent]))

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#Context, out: Collector[Warning]): Unit = {
    // 获取状态
    val loginFailList: lang.Iterable[LoginEvent] = loginFailState.get()
    // 判断状态是否是fail
    if (value.eventType == "fail") {
      // 如果是fail，再判断是否是第一个登陆失败事件，如果是的话，注册定时器，并存在状态中
      if (!loginFailList.iterator().hasNext) {
        ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 2000L)
      }
      loginFailState.add(value)
    } else {
      // 如果状态成功，则清空状态
      loginFailState.clear()
    }


  }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, Warning]#OnTimerContext, out: Collector[Warning]): Unit = {
      // 触发定时器的时候，根据状态里的失败个数决定是否输出报警
      val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]()
      val iter = loginFailState.get().iterator()
      while(iter.hasNext){
        allLoginFails += iter.next()
      }

      // 判断个数
      if( allLoginFails.length >= maxFailTimes ){
        out.collect( Warning( allLoginFails.head.userId, allLoginFails.head.eventTime, allLoginFails.last.eventTime, "login fail in 2 seconds for " + allLoginFails.length + " times." ) )
      }
      // 清空状态
      loginFailState.clear()
    }
}
