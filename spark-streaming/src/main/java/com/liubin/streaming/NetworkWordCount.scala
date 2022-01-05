package com.liubin.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NetworkWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    // 采集周期
    val ssc = new StreamingContext(conf, Seconds(3))
    // 获取socket数据
    val lineDStream = ssc.socketTextStream("172.20.247.28", 9999)
    val reduceDStream = lineDStream.flatMap((str: String) => str.split(" ")).map((_, 1)).reduceByKey(_ + _)
    reduceDStream.print()
    // 启动采集器
    ssc.start()
    // 等待采集器关闭
    ssc.awaitTermination()

  }

}
