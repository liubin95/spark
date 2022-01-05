package com.liubin.streaming

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.Random

object MyReceivcer {

  // 自定义数据源
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("MyReceivcer")
    // 采集周期
    val ssc = new StreamingContext(conf, Seconds(3))
    val inputStream: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceivcers())

    inputStream.print()

    // 启动采集器
    ssc.start()
    // 等待采集器关闭
    ssc.awaitTermination()
  }

  // 继承Receiver
  // 确定输出泛型
  // 确定存储类型
  class MyReceivcers extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    private var flag = true

    override def onStart(): Unit = {
      //启动一个线程
      new Thread(() => {
        // 采集数据
        while (flag) {
          val string = new Random().nextInt().toString
          // 存储数据
          store(string)
          Thread.sleep(1000)
        }
      }).start()
    }

    override def onStop(): Unit = flag = false
  }

}
