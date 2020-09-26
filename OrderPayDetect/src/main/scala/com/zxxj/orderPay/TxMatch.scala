package com.zxxj.orderPay

import java.net.URL

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * @author shkstart
 * @create 2020-09-27 6:50
 *
 */
// 定义到账事件的样例类
case class ReceiptEvent(txId: String, payChannel: String, timestamp: Long)


object TxMatch {
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
      .filter(_.txId != "") // 只要pay事件
      .keyBy(_.txId)


    val receiptResource: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: DataStream[ReceiptEvent] = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)

    // 2. 连接两条流，做分别计算
    val resultStream = orderEventStream.connect(receiptEventStream)
      .process(new TxPayMatchDetect())

    // 3. 定义不匹配的侧输出流标签
    val unmatchedPays = new OutputTag[OrderEvent]("unmatched-pays")
    val unmatchedReceipts = new OutputTag[ReceiptEvent]("unmatched-receipts")

    // 4. 打印输出
    resultStream.print("matched")


    env.execute("tx matched job")
  }

}

// 自定义CoProcessFunction,用状态保存另一条流已来的数据
class TxPayMatchDetect() extends CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)] {

  //定义状态，用来保存已经来到的 pay事件和 ceceipt事件
  lazy val payEventState: ValueState[OrderEvent] = getRuntimeContext.getState(new ValueStateDescriptor[OrderEvent]("payEvent", classOf[OrderEvent]))
  lazy val receiptEventState: ValueState[ReceiptEvent] = getRuntimeContext.getState(new ValueStateDescriptor[ReceiptEvent]("ReceiptEvent", classOf[ReceiptEvent]))

  override def processElement1(pay: OrderEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 订单支付事件来了，要考察当前是否已经来过receipt
    val receipt: ReceiptEvent = receiptEventState.value()
    if (receipt != null) {
      // 如果来过，正常匹配输出到主流
      out.collect((pay, receipt))
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 2. 如果receipt 还没来，注册定时器，等待5s钟
      ctx.timerService().registerEventTimeTimer(pay.timestamp * 1000L + 5000L)
      // 更新状态
      payEventState.update(pay)
    }
  }

  override def processElement2(receipt: ReceiptEvent, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 到账事件来了，要考察当前是否已经来过pay
    val pay: OrderEvent = payEventState.value()

    if (pay != null) {
      // 如果来过，正常匹配输出到主流
      out.collect((pay, receipt))
      // 清空状态
      payEventState.clear()
      receiptEventState.clear()
    } else {
      // 2. 如果receipt 还没来，注册定时器，等待5s钟
      ctx.timerService().registerEventTimeTimer(receipt.timestamp * 1000L + 5000L)
      // 更新状态
      payEventState.update(pay)
    }


  }

  // 定时器触发，需要判断状态中的值
  override def onTimer(timestamp: Long, ctx: CoProcessFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#OnTimerContext, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 判断两个状态，哪个不为空，那么就是另一个没来
    if (payEventState.value() != null) {
      ctx.output(new OutputTag[OrderEvent]("unmatched-pays"), payEventState.value())

    }
    if (receiptEventState.value() != null) {
      ctx.output(new OutputTag[ReceiptEvent]("unmatched-receipts"), receiptEventState.value())
    }

    // 清空状态
    payEventState.clear()
    receiptEventState.clear()
  }
}