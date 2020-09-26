package com.zxxj.orderPay


/**
 * @author shkstart
 * @create 2020-09-22 7:19
 */


import java.net.URL
import java.util

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


// 定义输入输出样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)

case class OrderResult(orderId: Long, status: String)


class OrderTimeOut {
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

    // 2. 定义一个pattern
    val orderPayPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      // 模式序列连接的时候，采用宽松近邻的模式
      .followedBy("pay").where(_.eventType == "pay")
      .within(Time.minutes(15))


    // 3. 将Pattern应用到 按照orderId分组后的datastream
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream.keyBy(_.orderId), orderPayPattern)

    // 4.定义一个超时订单侧输出流的标签
    val orderTimeOutputTag = new OutputTag[OrderResult]("timeout")

    // 5. 调用select 方法，分别处理匹配数据和超时未匹配数据
    val resultStream: DataStream[OrderResult] = patternStream
      .select(orderTimeOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())


    // 6.打印输出
    resultStream.print("pay")
    resultStream.getSideOutput(orderTimeOutputTag).print("timeout")

    // 执行
    env.execute("order timeout job")


  }


}

// 实现自定义的PatternTimeoutFunction
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId: Long = pattern.get("create").iterator().next().orderId
    OrderResult(timeoutOrderId, "timeout")

  }
}

//实现自定义的PatternSelectFunction
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderResult] {
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val payedOrderId: Long = pattern.get("pay").iterator().next().orderId
    OrderResult(payedOrderId, "payed")
  }
}