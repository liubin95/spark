package com.liubin

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Stop {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Stop")
    val ssc = new StreamingContext(conf, Seconds(3))
    val lineDStream = ssc.socketTextStream("172.24.33.19", 9999)
    val word = lineDStream.map((_, 1))
    word.reduceByKey(_ + _).print()
    // 启动采集器
    ssc.start()

    new Thread(new Runnable {
      override def run(): Unit =
        Thread.sleep(12000)

      // stopGracefully 优雅的关闭
      // 处理完数据再结束
      ssc.stop(stopSparkContext = true, stopGracefully = true)

    }).start()

    // 等待采集器关闭 - 阻塞main线程
    ssc.awaitTermination()
  }
}
