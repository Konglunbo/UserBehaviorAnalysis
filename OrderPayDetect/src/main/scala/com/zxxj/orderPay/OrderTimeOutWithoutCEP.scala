package com.zxxj.orderPay

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author shkstart
 * @create 2020-09-25 6:16
 */
class OrderTimeOutWithoutCEP {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 数据读取 转换成样例类
    val resource: URL = getClass.getResource("/OrderPayDetect/src/main/resources/OrderLog.csv")
    val orderEventStream: DataStream[OrderEvent] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)


    // 按照OrderId分组处理，主流输出正常支付的结果，侧输出流输出订单超时的结果
    val orderResultStream: DataStream[OrderResult] = orderEventStream
      .keyBy(_.orderId)
      .process(new OrderPayMatchDetect())

    // 打印输出
    orderResultStream.print("payed")
    orderResultStream.getSideOutput(new OutputTag[OrderResult]("timeout")).print("timeout")

    env.execute("order time without cep job")


  }

}


// 自定义 ProcessFunction,按照当前数据的类型以及之前的状态，判断要做的操作
class OrderPayMatchDetect() extends KeyedProcessFunction[Long, OrderEvent, OrderResult] {

  // 定义状态，用来保存之前的 create 和 pay事件 是否已经来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-payed", classOf[Boolean]))
  lazy val isCreatedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("is-created", classOf[Boolean]))

  // 定义状态，保存定时器时间戳

  lazy val timerTsState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts", classOf[Long]))

  //定义测输出流，用于输出超时订单结果
  val orderTimeOutputTag = new OutputTag[OrderResult]("timeout")

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#Context, out: Collector[OrderResult]): Unit = {

    // 先拿到当前状态
    val isPayed: Boolean = isPayedState.value()
    val isCreated: Boolean = isCreatedState.value()
    val timerTs: Long = timerTsState.value()

    // 判断当前事件的类型以及之前的状态，不同的组合，有不同的处理流程
    // 情况1 ： 来的是create ,接下来判断是否Pay过
    if (value.eventType == "create") {
      // 1.1 如果已经pay过，匹配成功，输出到主流
      if (isPayed) {
        out.collect(OrderResult(value.orderId, "payed"))
        // 已经输出结果了，清空状态和定时器
        isPayedState.clear()
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 1.2 如果还没有pay过，要注册一个定时器，等待15min,开始等待
      else {
        val ts: Long = value.timestamp * 1000L + 15 * 60 * 1000L
        ctx.timerService().registerEventTimeTimer(ts)
        // 更新状态
        timerTsState.update(ts)
        isCreatedState.update(true)
      }

    }

    // 2 如果来的是pay,那么要继续判断是否已经create过
    else if (value.eventType == "pay") {
      // 2.1 如果已经create，还需要判断create 和 pay 的时间差是否超过15min
      if (isCreated) {
        // 如果没有超时，输出正常的结果
        if (value.timestamp * 1000L <= timerTs) {
          out.collect(OrderResult(value.orderId, "payed"))
          // 已经输出结果了，清空状态和定时器
          isPayedState.clear()
          isCreatedState.clear()
          timerTsState.clear()
          ctx.timerService().deleteEventTimeTimer(timerTs)

        }
        // 2.1.2 如果已经超时，侧输出流输出超时结果
        else {
          ctx.output(orderTimeOutputTag, OrderResult(value.orderId, "payed but already timeout"))
        }
        // 已经输出结果了，清空状态和定时器
        isPayedState.clear()
        isCreatedState.clear()
        timerTsState.clear()
        ctx.timerService().deleteEventTimeTimer(timerTs)
      }
      // 2.2 如果没有create 过， 可能create 丢失，也可能乱序,需要等待create
      else {
        // 注册一个当前Pay时间时间戳的定时器
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L)
        //更新状态
        timerTsState.update(value.timestamp * 1000L)
        isPayedState.update(true)
      }
    }


  }

  // 定时器触发，肯定有一个事件没等到，输出一个异常信息
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderResult]#OnTimerContext, out: Collector[OrderResult]): Unit = {

    if (isPayedState.value()) {
      // 如果isPayedState 为 true , 说明一定是 pay 先来，而且没有等到create
      ctx.output(orderTimeOutputTag, OrderResult(ctx.getCurrentKey, "payed but not created "))

    } else {
      // 如果isPayedState 为 false , 说明是 create先来，等pay 没有等到
      ctx.output(orderTimeOutputTag, OrderResult(ctx.getCurrentKey, "timeout"))

    }
    // 已经输出结果了，清空状态
    isPayedState.clear()
    isCreatedState.clear()
    timerTsState.clear()

  }
}