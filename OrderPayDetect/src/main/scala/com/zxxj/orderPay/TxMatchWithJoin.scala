package com.zxxj.orderPay

import java.net.URL

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @author shkstart
 * @create 2020-09-27 21:18
 */
object TxMatchWithJoin {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    // 数据读取 转换成样例类
    val resource: URL = getClass.getResource("/OrderPayDetect/src/main/resources/OrderLog.csv")
    val orderEventStream: KeyedStream[OrderEvent, String] = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        OrderEvent(dataArray(0).toLong, dataArray(1), dataArray(2), dataArray(3).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)
      .filter(_.txId != "") // 只要pay事件
      .keyBy(_.txId)



    val receiptResource: URL = getClass.getResource("/ReceiptLog.csv")
    val receiptEventStream: KeyedStream[ReceiptEvent, String] = env.readTextFile(receiptResource.getPath)
      .map(data => {
        val dataArray: Array[String] = data.split(",")
        ReceiptEvent(dataArray(0), dataArray(1), dataArray(2).toLong)
      }).assignAscendingTimestamps(_.timestamp * 1000L)
      .keyBy(_.txId)


    // 2. 连接两条流，做分别计算
    val resultStream: DataStream[(OrderEvent, ReceiptEvent)] = orderEventStream
      .intervalJoin(receiptEventStream)
      .between(Time.seconds(-3), Time.seconds(5))
      .process(new TxPayMatchDetectByJoin())



    resultStream.print("result")


    env.execute("tx matched job")
  }

}


// 自定义 ProcessJoinFunction
class TxPayMatchDetectByJoin() extends  ProcessJoinFunction[OrderEvent,ReceiptEvent,(OrderEvent,ReceiptEvent)] {
  override def processElement(left: OrderEvent, right: ReceiptEvent, ctx: ProcessJoinFunction[OrderEvent, ReceiptEvent, (OrderEvent, ReceiptEvent)]#Context, out: Collector[(OrderEvent, ReceiptEvent)]): Unit = {
    // 只能检测匹配输出

    out.collect(left,right)



  }
}